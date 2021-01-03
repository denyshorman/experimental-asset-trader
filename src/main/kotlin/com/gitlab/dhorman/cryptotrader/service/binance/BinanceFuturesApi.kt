package com.gitlab.dhorman.cryptotrader.service.binance

import com.gitlab.dhorman.cryptotrader.util.*
import com.gitlab.dhorman.cryptotrader.util.serializer.*
import com.gitlab.dhorman.cryptotrader.util.signer.HmacSha256Signer
import com.gitlab.dhorman.cryptotrader.util.signer.Signer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.flux
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.*
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.descriptors.*
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.json.*
import mu.KotlinLogging
import org.springframework.http.HttpMethod
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.client.WebSocketClient
import java.io.File
import java.math.BigDecimal
import java.net.URI
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.ClosedChannelException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.HashMap
import kotlin.time.minutes
import kotlin.time.seconds

class BinanceFuturesApi(private val config: Config) {
    private val logger = KotlinLogging.logger {}
    private val signer: Signer
    private val json: Json = config.json

    private val apiUrl: String
    private val apiUrlStream: String

    private val webClient: WebClient = config.webClient
    private val webSocketClient: WebSocketClient = config.webSocketClient

    private val streamCache = ConcurrentHashMap<String, Flow<*>>()

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("BinanceFuturesApi"))
    private val closed = AtomicBoolean(false)

    private val exchangeInfoFileChannel: AsynchronousFileChannel?
    private val wsListenKeyFileChannel: AsynchronousFileChannel?

    init {
        signer = HmacSha256Signer(config.apiSecret, toHexString)

        when (config.apiNet) {
            Config.ApiNet.Main -> {
                apiUrl = API_URL_MAIN_NET
                apiUrlStream = API_STREAM_URL_MAIN_NET
            }
            Config.ApiNet.Test -> {
                apiUrl = API_URL_TEST_NET
                apiUrlStream = API_STREAM_URL_TEST_NET
            }
        }

        if (config.fileCachePath != null) {
            try {
                Files.createDirectories(Paths.get(config.fileCachePath))

                val exchangeInfoFilePath = Paths.get("${config.fileCachePath}${File.separator}exchange_info.json")
                exchangeInfoFileChannel = AsynchronousFileChannel.open(exchangeInfoFilePath, *FILE_OPTIONS)

                val wsListenKeyFilePath = Paths.get("${config.fileCachePath}${File.separator}ws_listen_key.json")
                wsListenKeyFileChannel = AsynchronousFileChannel.open(wsListenKeyFilePath, *FILE_OPTIONS)
            } catch (e: Throwable) {
                throw e
            }
        } else {
            exchangeInfoFileChannel = null
            wsListenKeyFileChannel = null
        }
    }

    //region Maintenance
    suspend fun close() {
        if (closed.getAndSet(true)) return
        scope.coroutineContext[Job]?.cancelAndJoin()

        if (exchangeInfoFileChannel != null) ignoreErrors { exchangeInfoFileChannel.close() }
        if (wsListenKeyFileChannel != null) ignoreErrors { wsListenKeyFileChannel.close() }
    }
    //endregion

    //region Market Data API
    suspend fun ping() {
        return callApi("/fapi/v1/ping", HttpMethod.GET, emptyMap(), false, false, serializer())
    }

    suspend fun getCurrentServerTime(): ServerTimeResp {
        return callApi("/fapi/v1/time", HttpMethod.GET, emptyMap(), false, false, serializer())
    }

    suspend fun getExchangeInfo(): ExchangeInfo {
        return callApi("/fapi/v1/exchangeInfo", HttpMethod.GET, emptyMap(), false, false, serializer())
    }
    //endregion

    //region Account/Trades API
    suspend fun getCommissionRate(symbol: String, timestamp: Instant = Instant.now(), recvWindow: Long? = null): JsonElement {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return callApi("/fapi/v1/commissionRate", HttpMethod.GET, params, true, true, serializer())
    }
    //endregion

    //region Market Streams
    fun aggregateTradeStream(symbol: String): Flow<EventData<AggregateTradeEvent>> {
        return cacheStream("${symbol.toLowerCase()}@aggTrade") { subscribeTo(it, serializer()) }
    }

    fun tradeStream(symbol: String): Flow<EventData<TradeEvent>> {
        return cacheStream("${symbol.toLowerCase()}@trade") { subscribeTo(it, serializer()) }
    }

    fun markPriceStream(symbol: String, updateSpeed: MarkPriceUpdateSpeed? = null): Flow<EventData<MarkPriceEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeSec}s"
        return cacheStream("${symbol.toLowerCase()}@markPrice$updateSpeedStr") { subscribeTo(it, serializer()) }
    }

    fun allMarketsMarkPriceStream(updateSpeed: MarkPriceUpdateSpeed? = null): Flow<EventData<List<MarkPriceEvent>>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeSec}s"
        return cacheStream("!markPrice@arr$updateSpeedStr") { subscribeTo(it, serializer()) }
    }

    fun candlestickStream(symbol: String, interval: CandleStickInterval): Flow<EventData<CandlestickEvent>> {
        return cacheStream("${symbol.toLowerCase()}@kline_${interval.id}") { subscribeTo(it, serializer()) }
    }

    fun continuousContractCandlestickStream(symbol: String, contractType: ContractType, interval: CandleStickInterval): Flow<EventData<ContinuousCandlestickEvent>> {
        return cacheStream(
            "${{ symbol.toLowerCase() }}_${
                contractType.toString().toLowerCase()
            }@continuousKline_${interval.id}"
        ) { subscribeTo(it, serializer()) }
    }

    fun individualSymbolMiniTickerStream(symbol: String): Flow<EventData<MiniTickerEvent>> {
        return cacheStream("${symbol.toLowerCase()}@miniTicker") { subscribeTo(it, serializer()) }
    }

    val allMarketMiniTickersStream: Flow<EventData<List<MiniTickerEvent>>> = run {
        subscribeTo("!miniTicker@arr", ListSerializer(MiniTickerEvent.serializer()))
            .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    fun individualSymbolTickerStream(symbol: String): Flow<EventData<TickerEvent>> {
        return cacheStream("${symbol.toLowerCase()}@ticker") { subscribeTo(it, serializer()) }
    }

    val allMarketTickersStream: Flow<EventData<List<TickerEvent>>> = run {
        subscribeTo("!ticker@arr", ListSerializer(TickerEvent.serializer()))
            .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    fun individualSymbolBookTickerStream(symbol: String): Flow<EventData<BookTickerEvent>> {
        return cacheStream("${symbol.toLowerCase()}@bookTicker") { subscribeTo(it, serializer()) }
    }

    val allBookTickerStream: Flow<EventData<BookTickerEvent>> = run {
        subscribeTo("!bookTicker", serializer<BookTickerEvent>())
            .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    fun liquidationOrderStream(symbol: String): Flow<EventData<LiquidationOrderEvent>> {
        return cacheStream("${symbol.toLowerCase()}@forceOrder") { subscribeTo(it, serializer()) }
    }

    val allMarketLiquidationOrderStream: Flow<EventData<LiquidationOrderEvent>> = run {
        subscribeTo("!forceOrder@arr", serializer<LiquidationOrderEvent>())
            .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    fun partialBookDepthStream(
        symbol: String,
        level: PartialBookDepthEvent.Level,
        updateSpeed: BookUpdateSpeed? = null,
    ): Flow<EventData<PartialBookDepthEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeMs}ms"
        return cacheStream("${symbol.toLowerCase()}@depth${level.id}$updateSpeedStr") { subscribeTo(it, serializer()) }
    }

    fun diffDepthStream(
        symbol: String,
        updateSpeed: BookUpdateSpeed? = null,
    ): Flow<EventData<DiffDepthEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeMs}ms"
        return cacheStream("${symbol.toLowerCase()}@depth$updateSpeedStr") { subscribeTo(it, serializer()) }
    }

    fun compositeIndexStream(symbol: String): Flow<EventData<CompositeIndexEvent>> {
        return cacheStream("${symbol.toLowerCase()}@compositeIndex") { subscribeTo(it, serializer()) }
    }
    //endregion

    //region User Data Streams
    val accountStream: Flow<EventData<AccountEvent>> = run {
        subscribeToPrivateChannel().shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }
    //endregion

    //region Public Models
    data class Config(
        val apiKey: String,
        val apiSecret: String,
        val apiNet: ApiNet,
        val fileCachePath: String? = null,
        val webClient: WebClient = springWebClient(
            connectTimeoutMs = 5000,
            readTimeoutMs = 5000,
            writeTimeoutMs = 5000,
            maxInMemorySize = 2 * 1024 * 1024,
        ),
        val webSocketClient: WebSocketClient = springWebsocketClient(
            connectTimeoutMs = 10000,
            readTimeoutMs = 5000,
            writeTimeoutMs = 5000,
            maxFramePayloadLength = 65536 * 4,
        ),
        val json: Json = Json {
            ignoreUnknownKeys = true
        },
    ) {
        enum class ApiNet {
            Main,
            Test,
        }
    }

    enum class SymbolType {
        FUTURE,
    }

    enum class ContractType {
        PERPETUAL,
    }

    enum class OrderType {
        LIMIT,
        MARKET,
        STOP,
        STOP_MARKET,
        TAKE_PROFIT,
        TAKE_PROFIT_MARKET,
        TRAILING_STOP_MARKET,
        LIQUIDATION,
    }

    enum class OrderSide {
        BUY,
        SELL,
    }

    enum class OrderStatus {
        NEW,
        PARTIALLY_FILLED,
        FILLED,
        CANCELED,
        REJECTED,
        EXPIRED,
        NEW_INSURANCE,
        NEW_ADL,
    }

    enum class PositionSide {
        BOTH,
        LONG,
        SHORT,
    }

    @Serializable
    enum class TimeInForce {
        @SerialName("GTC")
        GOOD_TILL_CANCEL,

        @SerialName("IOC")
        IMMEDIATE_OR_CANCEL,

        @SerialName("FOK")
        FILL_OR_KILL,

        @SerialName("GTX")
        POST_ONLY,
    }

    enum class ExecutionType {
        NEW,
        PARTIAL_FILL,
        FILL,
        CANCELED,
        CALCULATED,
        EXPIRED,
        TRADE,
    }

    enum class WorkingType {
        MARK_PRICE,
        CONTRACT_PRICE,
    }

    enum class ResponseType {
        ACK,
        RESULT,
    }

    @Serializable
    enum class CandleStickInterval(val id: String) {
        @SerialName("1m")
        INTERVAL_1_MINUTE("1m"),

        @SerialName("3m")
        INTERVAL_3_MINUTES("3m"),

        @SerialName("5m")
        INTERVAL_5_MINUTES("5m"),

        @SerialName("15m")
        INTERVAL_15_MINUTES("15m"),

        @SerialName("30m")
        INTERVAL_30_MINUTES("30m"),

        @SerialName("1h")
        INTERVAL_1_HOUR("1h"),

        @SerialName("2h")
        INTERVAL_2_HOURS("2h"),

        @SerialName("4h")
        INTERVAL_4_HOURS("4h"),

        @SerialName("6h")
        INTERVAL_6_HOURS("6h"),

        @SerialName("8h")
        INTERVAL_8_HOURS("8h"),

        @SerialName("12h")
        INTERVAL_12_HOURS("12h"),

        @SerialName("1d")
        INTERVAL_1_DAY("1d"),

        @SerialName("3d")
        INTERVAL_3_DAYS("3d"),

        @SerialName("1w")
        INTERVAL_1_WEEK("1w"),

        @SerialName("1M")
        INTERVAL_1_MONTH("1M"),
    }

    enum class RateLimitType {
        REQUEST_WEIGHT,
        ORDERS,
    }

    enum class RateLimitInterval {
        MINUTE,
    }

    enum class ExchangeFilterType {
        PRICE_FILTER,
        LOT_SIZE,
        MARKET_LOT_SIZE,
        MAX_NUM_ORDERS,
        MAX_NUM_ALGO_ORDERS,
        PERCENT_PRICE,
    }

    @Serializable
    data class ServerTimeResp(
        @Serializable(InstantAsLongMillisSerializer::class) val serverTime: Instant,
    )

    @Serializable
    data class ExchangeInfo(
        val timezone: String,
        @Serializable(InstantAsLongMillisSerializer::class) val serverTime: Instant,
        val futuresType: FuturesType,
        val rateLimits: List<RateLimit>,
        val exchangeFilters: List<ExchangeFilter>,
        val symbols: List<Symbol>,
    ) {
        val symbolsIndexed: Map<String, Symbol> by lazy(LazyThreadSafetyMode.PUBLICATION) {
            val map = HashMap<String, Symbol>(symbols.size, 1.0F)
            for (symbol in symbols) {
                map[symbol.symbol] = symbol
            }
            map
        }

        enum class FuturesType {
            U_MARGINED,
        }

        @Serializable(ExchangeFilter.Serializer::class)
        sealed class ExchangeFilter {
            @Serializable
            data class PriceFilter(
                @Serializable(BigDecimalAsStringSerializer::class) val minPrice: BigDecimal,
                @Serializable(BigDecimalAsStringSerializer::class) val maxPrice: BigDecimal,
                @Serializable(BigDecimalAsStringSerializer::class) val tickSize: BigDecimal,
            ) : ExchangeFilter()

            @Serializable
            data class LotSizeFilter(
                @Serializable(BigDecimalAsStringSerializer::class) val minQty: BigDecimal,
                @Serializable(BigDecimalAsStringSerializer::class) val maxQty: BigDecimal,
                @Serializable(BigDecimalAsStringSerializer::class) val stepSize: BigDecimal,
            ) : ExchangeFilter()

            @Serializable
            data class MarketLotSizeFilter(
                @Serializable(BigDecimalAsStringSerializer::class) val minQty: BigDecimal,
                @Serializable(BigDecimalAsStringSerializer::class) val maxQty: BigDecimal,
                @Serializable(BigDecimalAsStringSerializer::class) val stepSize: BigDecimal,
            ) : ExchangeFilter()

            @Serializable
            data class MaxNumOrdersFilter(
                val limit: Long,
            ) : ExchangeFilter()

            @Serializable
            data class MaxNumAlgoOrdersFilter(
                val limit: Long,
            ) : ExchangeFilter()

            @Serializable
            data class PercentPriceFilter(
                @Serializable(BigDecimalAsStringSerializer::class) val multiplierUp: BigDecimal,
                @Serializable(BigDecimalAsStringSerializer::class) val multiplierDown: BigDecimal,
                val multiplierDecimal: Int,
            ) : ExchangeFilter()

            object Serializer : JsonContentPolymorphicSerializer<ExchangeFilter>(ExchangeFilter::class) {
                override fun selectDeserializer(element: JsonElement): DeserializationStrategy<out ExchangeFilter> {
                    return when (ExchangeFilterType.valueOf(element.jsonObject["filterType"]!!.jsonPrimitive.content)) {
                        ExchangeFilterType.PRICE_FILTER -> PriceFilter.serializer()
                        ExchangeFilterType.LOT_SIZE -> LotSizeFilter.serializer()
                        ExchangeFilterType.MARKET_LOT_SIZE -> MarketLotSizeFilter.serializer()
                        ExchangeFilterType.MAX_NUM_ORDERS -> MaxNumOrdersFilter.serializer()
                        ExchangeFilterType.MAX_NUM_ALGO_ORDERS -> MaxNumAlgoOrdersFilter.serializer()
                        ExchangeFilterType.PERCENT_PRICE -> PercentPriceFilter.serializer()
                    }
                }
            }
        }

        @Serializable
        data class RateLimit(
            val rateLimitType: RateLimitType,
            val interval: RateLimitInterval,
            val intervalNum: Long,
            val limit: Long,
        )

        @Serializable
        data class Symbol(
            val symbol: String,
            val pair: String,
            val contractType: ContractType,
            @Serializable(InstantAsLongMillisSerializer::class) val deliveryDate: Instant,
            @Serializable(InstantAsLongMillisSerializer::class) val onboardDate: Instant,
            val status: Status,
            @Serializable(BigDecimalAsStringSerializer::class) @SerialName("maintMarginPercent") val maintenanceMarginPercent: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val requiredMarginPercent: BigDecimal,
            val baseAsset: String,
            val quoteAsset: String,
            val marginAsset: String,
            val pricePrecision: Int,
            val quantityPrecision: Int,
            val baseAssetPrecision: Int,
            val quotePrecision: Int,
            val underlyingType: UnderlyingType,
            val underlyingSubType: List<UnderlyingSubType>,
            val settlePlan: Int,
            @Serializable(BigDecimalAsStringSerializer::class) val triggerProtect: BigDecimal,
            val filters: List<ExchangeFilter>,
            val orderTypes: List<OrderType>,
            val timeInForce: List<TimeInForce>,
        ) {
            enum class Status {
                TRADING,
            }

            enum class UnderlyingType {
                COIN,
                INDEX,
            }

            enum class UnderlyingSubType {
                DEFI,
            }
        }
    }

    @Serializable
    data class OrderBook(
        val lastUpdateId: Long,
        val bids: List<@Serializable(Record.RecordDeserializer::class) Record>,
        val asks: List<@Serializable(Record.RecordDeserializer::class) Record>,
    ) {
        data class Record(
            val price: BigDecimal,
            val qty: BigDecimal,
        ) {
            object RecordDeserializer : KSerializer<Record> {
                override val descriptor: SerialDescriptor = buildSerialDescriptor(
                    serialName = "BinanceFuturesOrderBookRecordDeserializer",
                    kind = StructureKind.LIST,
                ) {
                    element("price", BigDecimalAsStringSerializer.descriptor)
                    element("qty", BigDecimalAsStringSerializer.descriptor)
                }

                override fun deserialize(decoder: Decoder): Record {
                    return decoder.decodeStructure(descriptor) {
                        var price: BigDecimal? = null
                        var qty: BigDecimal? = null

                        loop@ while (true) {
                            when (val index = decodeElementIndex(descriptor)) {
                                CompositeDecoder.DECODE_DONE -> break@loop
                                0 -> price = decodeSerializableElement(descriptor, index, BigDecimalAsStringSerializer)
                                1 -> qty = decodeSerializableElement(descriptor, index, BigDecimalAsStringSerializer)
                            }
                        }

                        Record(price!!, qty!!)
                    }
                }

                override fun serialize(encoder: Encoder, value: Record) = throw RuntimeException("Not implemented")
            }
        }
    }
    //endregion

    //region Public Events
    @Serializable
    data class AggregateTradeEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("a") val aggregateTradeId: Long,
        @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val price: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val quantity: BigDecimal,
        @SerialName("f") val firstTradeId: Long,
        @SerialName("l") val lastTradeId: Long,
        @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val tradeTime: Instant,
        @SerialName("m") val buyerMarketMaker: Boolean,
    )

    @Serializable
    data class TradeEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val tradeTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("t") val tradeId: Long,
        @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val price: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val quantity: BigDecimal,
        @SerialName("X") val orderType: OrderType,
        @SerialName("m") val buyerMarketMaker: Boolean,
    )

    @Serializable
    data class MarkPriceEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val markPrice: BigDecimal,
        @SerialName("P") @Serializable(BigDecimalAsStringSerializer::class) val estimatedSettlePrice: BigDecimal,
        @SerialName("i") @Serializable(BigDecimalAsStringSerializer::class) val indexPrice: BigDecimal,
        @SerialName("r") @Serializable(BigDecimalAsStringSerializer::class) val fundingRate: BigDecimal,
        @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val nextFundingTime: Instant,
    )

    @Serializable
    data class CandlestickEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("k") val data: Data,
    ) {
        @Serializable
        data class Data(
            @SerialName("t") @Serializable(InstantAsLongMillisSerializer::class) val klineStartTime: Instant,
            @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val klineCloseTime: Instant,
            @SerialName("s") val symbol: String,
            @SerialName("i") val interval: CandleStickInterval,
            @SerialName("f") val firstTradeId: Long,
            @SerialName("L") val lastTradeId: Long,
            @SerialName("o") @Serializable(BigDecimalAsStringSerializer::class) val openPrice: BigDecimal,
            @SerialName("c") @Serializable(BigDecimalAsStringSerializer::class) val closePrice: BigDecimal,
            @SerialName("h") @Serializable(BigDecimalAsStringSerializer::class) val highPrice: BigDecimal,
            @SerialName("l") @Serializable(BigDecimalAsStringSerializer::class) val lowPrice: BigDecimal,
            @SerialName("v") @Serializable(BigDecimalAsStringSerializer::class) val baseAssetVolume: BigDecimal,
            @SerialName("n") val tradesCount: Long,
            @SerialName("x") val klineClosed: Boolean,
            @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val quoteAssetVolume: BigDecimal,
            @SerialName("V") @Serializable(BigDecimalAsStringSerializer::class) val takerBuyBaseAssetVolume: BigDecimal,
            @SerialName("Q") @Serializable(BigDecimalAsStringSerializer::class) val takerBuyQuoteAssetVolume: BigDecimal,
            @SerialName("B") @Serializable(BigDecimalAsStringSerializer::class) val ignore: BigDecimal,
        )
    }

    @Serializable
    data class ContinuousCandlestickEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("ps") val symbol: String,
        @SerialName("ct") val contractType: ContractType,
        @SerialName("k") val data: Data,
    ) {
        @Serializable
        data class Data(
            @SerialName("t") @Serializable(InstantAsLongMillisSerializer::class) val klineStartTime: Instant,
            @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val klineCloseTime: Instant,
            @SerialName("i") val interval: CandleStickInterval,
            @SerialName("f") val firstTradeId: Long,
            @SerialName("L") val lastTradeId: Long,
            @SerialName("o") @Serializable(BigDecimalAsStringSerializer::class) val openPrice: BigDecimal,
            @SerialName("c") @Serializable(BigDecimalAsStringSerializer::class) val closePrice: BigDecimal,
            @SerialName("h") @Serializable(BigDecimalAsStringSerializer::class) val highPrice: BigDecimal,
            @SerialName("l") @Serializable(BigDecimalAsStringSerializer::class) val lowPrice: BigDecimal,
            @SerialName("v") @Serializable(BigDecimalAsStringSerializer::class) val baseAssetVolume: BigDecimal,
            @SerialName("n") val tradesCount: Long,
            @SerialName("x") val klineClosed: Boolean,
            @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val quoteAssetVolume: BigDecimal,
            @SerialName("V") @Serializable(BigDecimalAsStringSerializer::class) val takerBuyBaseAssetVolume: BigDecimal,
            @SerialName("Q") @Serializable(BigDecimalAsStringSerializer::class) val takerBuyQuoteAssetVolume: BigDecimal,
            @SerialName("B") @Serializable(BigDecimalAsStringSerializer::class) val ignore: BigDecimal,
        )
    }

    @Serializable
    data class MiniTickerEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("c") @Serializable(BigDecimalAsStringSerializer::class) val closePrice: BigDecimal,
        @SerialName("o") @Serializable(BigDecimalAsStringSerializer::class) val openPrice: BigDecimal,
        @SerialName("h") @Serializable(BigDecimalAsStringSerializer::class) val highPrice: BigDecimal,
        @SerialName("l") @Serializable(BigDecimalAsStringSerializer::class) val lowPrice: BigDecimal,
        @SerialName("v") @Serializable(BigDecimalAsStringSerializer::class) val totalTradedBaseAssetVolume: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val totalTradedQuoteAssetVolume: BigDecimal,
    )

    @Serializable
    data class TickerEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val priceChange: BigDecimal,
        @SerialName("P") @Serializable(BigDecimalAsStringSerializer::class) val priceChangePercent: BigDecimal,
        @SerialName("w") @Serializable(BigDecimalAsStringSerializer::class) val weightedAveragePrice: BigDecimal,
        @SerialName("c") @Serializable(BigDecimalAsStringSerializer::class) val lastPrice: BigDecimal,
        @SerialName("Q") @Serializable(BigDecimalAsStringSerializer::class) val lastQuantity: BigDecimal,
        @SerialName("o") @Serializable(BigDecimalAsStringSerializer::class) val openPrice: BigDecimal,
        @SerialName("h") @Serializable(BigDecimalAsStringSerializer::class) val highPrice: BigDecimal,
        @SerialName("l") @Serializable(BigDecimalAsStringSerializer::class) val lowPrice: BigDecimal,
        @SerialName("v") @Serializable(BigDecimalAsStringSerializer::class) val totalTradedBaseAssetVolume: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val totalTradedQuoteAssetVolume: BigDecimal,
        @SerialName("O") @Serializable(InstantAsLongMillisSerializer::class) val statisticsOpenTime: Instant,
        @SerialName("C") @Serializable(InstantAsLongMillisSerializer::class) val statisticsCloseTime: Instant,
        @SerialName("F") val firstTradeId: Long,
        @SerialName("L") val lastTradeId: Long,
        @SerialName("n") val tradesCount: Long,
    )

    @Serializable
    data class BookTickerEvent(
        @SerialName("e") val eventType: String,
        @SerialName("u") val orderBookUpdateId: Long,
        @SerialName("s") val symbol: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val tranTime: Instant,
        @SerialName("b") @Serializable(BigDecimalAsStringSerializer::class) val bestBidPrice: BigDecimal,
        @SerialName("B") @Serializable(BigDecimalAsStringSerializer::class) val bestBidQty: BigDecimal,
        @SerialName("a") @Serializable(BigDecimalAsStringSerializer::class) val bestAskPrice: BigDecimal,
        @SerialName("A") @Serializable(BigDecimalAsStringSerializer::class) val bestAskQty: BigDecimal,
    )

    @Serializable
    data class LiquidationOrderEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("o") val order: Order,
    ) {
        @Serializable
        data class Order(
            @SerialName("s") val symbol: String,
            @SerialName("S") val side: OrderSide,
            @SerialName("o") val type: OrderType,
            @SerialName("X") val status: OrderStatus,
            @SerialName("f") val timeInForce: TimeInForce,
            @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val price: BigDecimal,
            @SerialName("ap") @Serializable(BigDecimalAsStringSerializer::class) val averagePrice: BigDecimal,
            @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val originalQuantity: BigDecimal,
            @SerialName("l") @Serializable(BigDecimalAsStringSerializer::class) val lastFilledQuantity: BigDecimal,
            @SerialName("z") @Serializable(BigDecimalAsStringSerializer::class) val filledAccumulatedQuantity: BigDecimal,
            @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val tradeTime: Instant,
        )
    }

    enum class MarkPriceUpdateSpeed(val timeSec: Long) {
        TIME_1_SEC(1),
        TIME_3_SEC(3),
    }

    enum class BookUpdateSpeed(val timeMs: Long) {
        TIME_100_MS(100),
        TIME_250_MS(250),
        TIME_500_MS(500),
    }

    @Serializable
    data class PartialBookDepthEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val tradeTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("U") val u0: Long,
        @SerialName("u") val u1: Long,
        @SerialName("pu") val pu: Long,
        @SerialName("b") val bids: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>,
        @SerialName("a") val asks: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>,
    ) {
        enum class Level(val id: Long) {
            LEVEL_5(5),
            LEVEL_10(10),
            LEVEL_20(20),
        }
    }

    @Serializable
    data class DiffDepthEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("U") val u0: Long,
        @SerialName("u") val u1: Long,
        @SerialName("pu") val pu: Long,
        @SerialName("b") val bids: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>,
        @SerialName("a") val asks: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>,
    )

    @Serializable
    data class CompositeIndexEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val price: BigDecimal,
        @SerialName("c") val composition: List<Composition>,
    ) {
        @Serializable
        data class Composition(
            @SerialName("b") val baseAsset: String,
            @SerialName("w") @Serializable(BigDecimalAsStringSerializer::class) val quantityWeight: BigDecimal,
            @SerialName("W") @Serializable(BigDecimalAsStringSerializer::class) val percentageWeight: BigDecimal,
        )
    }

    @Serializable(AccountEvent.Serializer::class)
    sealed class AccountEvent {
        @Serializable
        data class ListenKeyExpiredEvent(
            @SerialName("e") val eventType: String,
            @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
        ) : AccountEvent()

        @Serializable
        data class MarginCallEvent(
            @SerialName("e") val eventType: String,
            @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
            @SerialName("cw") @Serializable(BigDecimalAsStringSerializer::class) val crossWalletBalance: BigDecimal? = null,
            @SerialName("p") val positions: List<Position>,
        ) : AccountEvent() {
            @Serializable
            data class Position(
                @SerialName("s") val symbol: String,
                @SerialName("ps") val positionSide: PositionSide,
                @SerialName("pa") @Serializable(BigDecimalAsStringSerializer::class) val positionAmount: BigDecimal,
                @SerialName("mt") val marginType: MarginType,
                @SerialName("iw") @Serializable(BigDecimalAsStringSerializer::class) val isolatedWallet: BigDecimal? = null,
                @SerialName("mp") @Serializable(BigDecimalAsStringSerializer::class) val markPrice: BigDecimal,
                @SerialName("up") @Serializable(BigDecimalAsStringSerializer::class) val unrealizedPnL: BigDecimal,
                @SerialName("mm") @Serializable(BigDecimalAsStringSerializer::class) val maintenanceMarginRequired: BigDecimal,
            ) {
                enum class MarginType { CROSSED, ISOLATED }
            }
        }

        @Serializable
        data class AccountUpdateEvent(
            @SerialName("e") val eventType: String,
            @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
            @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val transaction: Instant,
            @SerialName("a") val data: Data,
        ) : AccountEvent() {
            @Serializable
            data class Data(
                val eventReasonType: EventReasonType,
                val balances: List<Balance>,
                val positions: List<Position>,
            ) {
                enum class EventReasonType {
                    DEPOSIT,
                    WITHDRAW,
                    ORDER,
                    FUNDING_FEE,
                    WITHDRAW_REJECT,
                    ADJUSTMENT,
                    INSURANCE_CLEAR,
                    ADMIN_DEPOSIT,
                    ADMIN_WITHDRAW,
                    MARGIN_TRANSFER,
                    MARGIN_TYPE_CHANGE,
                    ASSET_TRANSFER,
                    OPTIONS_PREMIUM_FEE,
                    OPTIONS_SETTLE_PROFIT,
                }

                @Serializable
                enum class MarginType {
                    @SerialName("crossed")
                    CROSSED,

                    @SerialName("isolated")
                    ISOLATED,
                }

                @Serializable
                data class Balance(
                    @SerialName("a") val asset: String,
                    @SerialName("wb") @Serializable(BigDecimalAsStringSerializer::class) val walletBalance: BigDecimal,
                    @SerialName("cw") @Serializable(BigDecimalAsStringSerializer::class) val crossWalletBalance: BigDecimal,
                )

                @Serializable
                data class Position(
                    @SerialName("s") val symbol: String,
                    @SerialName("pa") @Serializable(BigDecimalAsStringSerializer::class) val positionAmount: BigDecimal,
                    @SerialName("ep") @Serializable(BigDecimalAsStringSerializer::class) val entryPrice: BigDecimal,
                    @SerialName("cr") @Serializable(BigDecimalAsStringSerializer::class) val preFeeAccumulatedRealized: BigDecimal,
                    @SerialName("up") @Serializable(BigDecimalAsStringSerializer::class) val unrealizedPnL: BigDecimal,
                    @SerialName("mt") val marginType: MarginType,
                    @SerialName("iw") @Serializable(BigDecimalAsStringSerializer::class) val isolatedWallet: BigDecimal? = null,
                    @SerialName("ps") val positionSide: PositionSide,
                )
            }
        }

        @Serializable
        data class OrderTradeUpdateEvent(
            @SerialName("e") val eventType: String,
            @SerialName("E") @Serializable(InstantAsLongMillisSerializer::class) val eventTime: Instant,
            @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val tranTime: Instant,
            @SerialName("o") val order: Order,
        ) : AccountEvent() {
            @Serializable
            data class Order(
                @SerialName("s") val symbol: String,
                @SerialName("i") val orderId: Long,
                @SerialName("c") val clientOrderId: String,
                @SerialName("S") val side: OrderSide,
                @SerialName("X") val orderStatus: OrderStatus,
                @SerialName("o") val orderType: OrderType,
                @SerialName("f") val timeInForce: TimeInForce,
                @SerialName("x") val executionType: ExecutionType,
                @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val originalQuantity: BigDecimal,
                @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val originalPrice: BigDecimal,
                @SerialName("ap") @Serializable(BigDecimalAsStringSerializer::class) val averagePrice: BigDecimal,
                @SerialName("sp") @Serializable(BigDecimalAsStringSerializer::class) val stopPrice: BigDecimal? = null,
                @SerialName("l") @Serializable(BigDecimalAsStringSerializer::class) val orderLastFilledQuantity: BigDecimal,
                @SerialName("z") @Serializable(BigDecimalAsStringSerializer::class) val orderFilledAccumulatedQuantity: BigDecimal,
                @SerialName("L") @Serializable(BigDecimalAsStringSerializer::class) val lastFilledPrice: BigDecimal,
                @SerialName("N") val commissionAsset: String? = null,
                @SerialName("n") @Serializable(BigDecimalAsStringSerializer::class) val commission: BigDecimal,
                @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val orderTradeTime: Instant,
                @SerialName("t") val tradeId: Long,
                @SerialName("b") @Serializable(BigDecimalAsStringSerializer::class) val bidsNotional: BigDecimal,
                @SerialName("a") @Serializable(BigDecimalAsStringSerializer::class) val askNotional: BigDecimal,
                @SerialName("m") val makerSide: Boolean,
                @SerialName("R") val reduceOnly: Boolean,
                @SerialName("wt") val stopPriceWorkingType: WorkingType,
                @SerialName("ot") val originalOrderType: OrderType,
                @SerialName("ps") val positionSide: PositionSide,
                @SerialName("cp") val conditionalOrder: Boolean,
                @SerialName("AP") @Serializable(BigDecimalAsStringSerializer::class) val activationPrice: BigDecimal? = null,
                @SerialName("cr") @Serializable(BigDecimalAsStringSerializer::class) val callbackRate: BigDecimal? = null,
                @SerialName("rp") @Serializable(BigDecimalAsStringSerializer::class) val realizedProfit: BigDecimal,
            )
        }

        object Serializer : JsonContentPolymorphicSerializer<AccountEvent>(AccountEvent::class) {
            override fun selectDeserializer(element: JsonElement): DeserializationStrategy<out AccountEvent> {
                return when (val type = element.jsonObject["e"]!!.jsonPrimitive.content) {
                    "listenKeyExpired" -> ListenKeyExpiredEvent.serializer()
                    "MARGIN_CALL" -> MarginCallEvent.serializer()
                    "ACCOUNT_UPDATE" -> AccountUpdateEvent.serializer()
                    "ORDER_TRADE_UPDATE" -> OrderTradeUpdateEvent.serializer()
                    else -> throw SerializationException("Can't decode message with type $type")
                }
            }
        }
    }
    //endregion

    //region Private Models
    private class ConnectionData(
        val requestChannel: Channel<InternalWebSocketRequest> = Channel(Channel.RENDEZVOUS),
        val responseChannelRegistry: ChannelRegistry = ChannelRegistry()
    ) {
        private val reqIdCounter = AtomicLong(0)
        fun generateId(): Long = reqIdCounter.getAndIncrement()

        fun isClosed(): Boolean {
            return requestChannel.isClosedForSend
        }

        suspend fun close(error: DisconnectedException) {
            requestChannel.close(error)
            responseChannelRegistry.close(error)
        }

        class ChannelRegistry {
            private val mutex = Mutex()
            private var closed = AtomicBoolean(false)
            private val registry = ConcurrentHashMap<String, Channel<WebSocketEvent>>()

            suspend fun register(channelKey: String, channel: Channel<WebSocketEvent>): Boolean {
                if (closed.get()) return false
                return mutex.withLock {
                    if (closed.get()) {
                        false
                    } else {
                        registry[channelKey] = channel
                        true
                    }
                }
            }

            fun get(channelKey: String): Channel<WebSocketEvent>? {
                return registry[channelKey]
            }

            fun remove(channelKey: String): Channel<WebSocketEvent>? {
                return registry.remove(channelKey)
            }

            suspend fun close(error: DisconnectedException) {
                mutex.withLock {
                    closed.set(true)
                    registry.forEachValue(1L) { it.close(error) }
                }
            }
        }
    }

    private enum class SubscribeToState {
        INIT,
        SUBSCRIBE,
        CONFIRM_SUBSCRIPTION,
        CONSUME_EVENTS,
        UNSUBSCRIBE,
        CONFIRM_UNSUBSCRIPTION,
        EXIT,
    }

    private data class LimitViolation(
        val violationTimeMs: Long,
        val waitTimeMs: Long
    ) {
        val endTimeMs = violationTimeMs + waitTimeMs
    }

    @Serializable
    private data class ListenKey(
        val listenKey: String
    )

    @Serializable
    private data class CachedListenKey(
        val listenKey: String,
        @Serializable(InstantAsLongMillisSerializer::class) val createTime: Instant = Instant.now()
    )

    @Serializable
    private data class ErrorMsg(
        val code: Long,
        val msg: String
    )

    private data class InternalWebSocketRequest(
        val request: WebSocketRequest,
        val responseChannel: Channel<WebSocketEvent>
    )

    @Serializable
    private data class WebSocketRequest(
        val method: Method,
        val params: List<String>,
        val id: Long
    ) {
        enum class Method {
            SUBSCRIBE,
            UNSUBSCRIBE,
            LIST_SUBSCRIPTIONS,
            GET_PROPERTY,
            SET_PROPERTY,
        }
    }

    private sealed class WebSocketEvent {
        @Serializable
        data class Push(
            val stream: String,
            val data: JsonElement
        ) : WebSocketEvent()

        @Serializable
        data class Response(
            val result: JsonElement,
            val id: Long
        ) : WebSocketEvent()

        @Serializable
        data class Error(
            val code: Long,
            val msg: String,
            val id: Long? = null
        ) : WebSocketEvent()
    }
    //endregion

    //region User Data Streams Private API
    private suspend fun getListenKey(): String {
        val resp = callApi("/fapi/v1/listenKey", HttpMethod.POST, emptyMap(), true, false, serializer<ListenKey>())
        return resp.listenKey
    }

    private suspend fun pingListenKey(listenKey: String) {
        val params = mapOf(Pair("listenKey", listenKey))
        callApi("/fapi/v1/listenKey", HttpMethod.PUT, params, true, false, serializer<Unit>())
    }

    private suspend fun deleteListenKey(listenKey: String) {
        val params = mapOf(Pair("listenKey", listenKey))
        callApi("/fapi/v1/listenKey", HttpMethod.DELETE, params, true, false, serializer<Unit>())
    }
    //endregion

    //region Private HTTP Logic
    private suspend fun <T> callApi(
        method: String,
        httpMethod: HttpMethod,
        params: Map<String, String>,
        requiresApiKey: Boolean,
        requiresSignature: Boolean,
        retType: DeserializationStrategy<T>,
    ): T {
        var paramsStr = ""
        if (params.isNotEmpty()) {
            paramsStr = params.toQueryString()
            if (requiresSignature) {
                val signature = signer.sign(paramsStr)
                paramsStr = paramsStr.appendToQueryString(SIGNATURE, signature)
            }
            paramsStr = paramsStr.appendToUri()
        }
        var request = webClient.method(httpMethod).uri("$apiUrl$method$paramsStr")
        if (requiresApiKey) {
            request = request.header(API_KEY_HEADER, config.apiKey)
        }

        return request.awaitExchange { response ->
            val data = response.awaitBody<String>()

            if (response.statusCode().is2xxSuccessful) {
                json.decodeFromString(retType, data)!!
            } else {
                val error = json.decodeFromString<ErrorMsg>(data)
                throw Exception(error.code, error.msg)
            }
        }
    }
    //endregion

    //region WebSocket Logic
    private val connection: Flow<ConnectionData> = run {
        channelFlow<ConnectionData> connection@{
            logger.debug("Starting Binance Futures connection channel")

            while (isActive) {
                logger.debug("Establishing connection with $apiUrlStream...")

                try {
                    var connectionData: ConnectionData? = null

                    val session = webSocketClient.execute(URI.create(apiUrlStream)) { session ->
                        mono(Dispatchers.Unconfined) {
                            logger.info("Connection established with $apiUrlStream")

                            coroutineScope {
                                val wsMsgCounter = AtomicLong(0)
                                val wsMsgReceiver = Channel<WebSocketMessage>(Channel.RENDEZVOUS)
                                val requestResponses = ConcurrentHashMap<Long, List<Channel<WebSocketEvent>>>()
                                connectionData = ConnectionData()

                                this@connection.send(connectionData!!)

                                // Messages consumer
                                launch(start = CoroutineStart.UNDISPATCHED) {
                                    session.receive().asFlow()
                                        .onEach { wsMsgCounter.incrementAndGet() }
                                        .filter { it.type == WebSocketMessage.Type.TEXT }
                                        .collect { msg ->
                                            val payloadJsonString = msg.payloadAsText
                                            if (logger.isTraceEnabled) logger.trace("Received: $payloadJsonString")

                                            val event = try {
                                                try {
                                                    json.decodeFromString<WebSocketEvent.Push>(payloadJsonString)
                                                } catch (e: SerializationException) {
                                                    try {
                                                        json.decodeFromString<WebSocketEvent.Response>(payloadJsonString)
                                                    } catch (e: SerializationException) {
                                                        val error = json.decodeFromString<WebSocketEvent.Error>(payloadJsonString)
                                                        if (error.id == null) {
                                                            throw error.toException()
                                                        } else {
                                                            error
                                                        }
                                                    }
                                                }
                                            } catch (e: Throwable) {
                                                logger.error("Can't handle websocket message: ${e.message}. Payload: $payloadJsonString")
                                                return@collect
                                            }

                                            when (event) {
                                                is WebSocketEvent.Push -> ignoreErrors { connectionData!!.responseChannelRegistry.get(event.stream)?.send(event) }
                                                is WebSocketEvent.Response -> requestResponses.remove(event.id)?.forEach { ignoreErrors { it.send(event) } }
                                                is WebSocketEvent.Error -> requestResponses.remove(event.id)?.forEach { ignoreErrors { it.send(event) } }
                                            }
                                        }
                                    throw ClosedChannelException()
                                }

                                // Message sender
                                launch(start = CoroutineStart.UNDISPATCHED) {
                                    val output = flux(Dispatchers.Unconfined) {
                                        val reqLimiter = SimpleRequestLimiter(5, 1000)
                                        for (msg in wsMsgReceiver) {
                                            delay(reqLimiter.waitMs())
                                            send(msg)
                                        }
                                    }
                                    session.send(output).awaitFirstOrNull()
                                    throw ClosedChannelException()
                                }

                                // Ping requests producer
                                launch {
                                    var prevTs = wsMsgCounter.get()
                                    while (isActive) {
                                        delay(2000)
                                        val currentTs = wsMsgCounter.get()
                                        if (currentTs == prevTs) {
                                            val pingMsg = session.pingMessage { it.wrap("ping".toByteArray()) }
                                            wsMsgReceiver.send(pingMsg)
                                        }
                                        prevTs = currentTs
                                    }
                                }

                                // Request messages aggregator
                                launch {
                                    val reqQueue = LinkedList<InternalWebSocketRequest>()

                                    while (isActive) {
                                        reqQueue.add(connectionData!!.requestChannel.receive())

                                        withTimeoutOrNull(250) {
                                            for (req in connectionData!!.requestChannel) reqQueue.add(req)
                                        }

                                        val groupedRequests = reqQueue
                                            .groupBy { it.request.method }
                                            .mapValues { (method, requests) ->
                                                when (method) {
                                                    WebSocketRequest.Method.SUBSCRIBE, WebSocketRequest.Method.UNSUBSCRIBE -> {
                                                        val channels = LinkedList<String>()
                                                        val reqResponses = LinkedList<Channel<WebSocketEvent>>()
                                                        for (req in requests) {
                                                            channels.addAll(req.request.params)
                                                            reqResponses.add(req.responseChannel)
                                                        }
                                                        val newReq = WebSocketRequest(method, channels, connectionData!!.generateId())
                                                        requestResponses[newReq.id] = reqResponses
                                                        listOf(newReq)
                                                    }
                                                    else -> {
                                                        val newRequests = LinkedList<WebSocketRequest>()
                                                        for (request in requests) {
                                                            newRequests.add(request.request)
                                                            requestResponses[request.request.id] = listOf(request.responseChannel)
                                                        }
                                                        newRequests
                                                    }
                                                }
                                            }

                                        reqQueue.clear()

                                        for ((_, requests) in groupedRequests) {
                                            for (request in requests) {
                                                val jsonStr = json.encodeToString(request)
                                                val webSocketMsg = session.textMessage(jsonStr)
                                                wsMsgReceiver.send(webSocketMsg)
                                            }
                                        }
                                    }
                                }
                            }

                            null
                        }
                    }

                    try {
                        session.awaitFirstOrNull()
                        throw ClosedChannelException()
                    } catch (e: Throwable) {
                        val error = DisconnectedException(e)
                        connectionData?.close(error)
                        throw error
                    }
                } catch (e: DisconnectedException) {
                    when (e.cause) {
                        is CancellationException -> {
                            // ignore
                        }
                        else -> {
                            logger.warn("${e.message}.${if (e.cause != null) " Cause: ${e.cause}" else ""}")
                            delay(1000)
                        }
                    }
                } finally {
                    logger.info("Connection closed with $apiUrlStream")
                }
            }

            logger.debug("Closing Binance Futures connection channel")
        }
            .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 1)
            .filter { !it.isClosed() }
    }

    private fun <T> subscribeTo(
        channel: String,
        payloadType: DeserializationStrategy<out T>,
    ): Flow<EventData<T>> = channelFlow {
        connection.conflate().collect { connection ->
            var state = SubscribeToState.INIT
            var eventData = EventData<T>()

            while (true) {
                when (state) {
                    SubscribeToState.INIT -> {
                        try {
                            withContext(NonCancellable) {
                                val registered = connection.responseChannelRegistry.register(channel, Channel(64))
                                state = if (registered) SubscribeToState.SUBSCRIBE else SubscribeToState.EXIT
                            }
                        } catch (e: CancellationException) {
                            state = SubscribeToState.EXIT
                        }
                    }
                    SubscribeToState.SUBSCRIBE -> {
                        val request = WebSocketRequest(WebSocketRequest.Method.SUBSCRIBE, listOf(channel), connection.generateId())
                        val internalRequest = InternalWebSocketRequest(request, connection.responseChannelRegistry.get(channel)!!)
                        state = try {
                            withContext(NonCancellable) {
                                connection.requestChannel.send(internalRequest)
                            }
                            SubscribeToState.CONFIRM_SUBSCRIPTION
                        } catch (e: CancellationException) {
                            SubscribeToState.CONFIRM_SUBSCRIPTION
                        } catch (e: DisconnectedException) {
                            SubscribeToState.EXIT
                        }
                    }
                    SubscribeToState.CONFIRM_SUBSCRIPTION -> {
                        try {
                            withContext(NonCancellable) {
                                val msg = try {
                                    withTimeout(10.seconds) {
                                        connection.responseChannelRegistry.get(channel)!!.receive()
                                    }
                                } catch (e: TimeoutCancellationException) {
                                    eventData = eventData.setError(Exception("Subscribe confirmation has not been received within specified timeout"))
                                    state = SubscribeToState.SUBSCRIBE
                                    this@channelFlow.send(eventData)
                                    return@withContext
                                } catch (e: DisconnectedException) {
                                    eventData = eventData.setError(e)
                                    state = SubscribeToState.EXIT
                                    this@channelFlow.send(eventData)
                                    return@withContext
                                }

                                try {
                                    when (msg) {
                                        is WebSocketEvent.Response -> {
                                            eventData = eventData.setSubscribed(true)
                                            state = SubscribeToState.CONSUME_EVENTS
                                            if (logger.isDebugEnabled) logger.debug("Subscribed to channel $channel")
                                        }
                                        is WebSocketEvent.Error -> {
                                            eventData = eventData.setError(msg.toException())
                                            state = SubscribeToState.SUBSCRIBE
                                        }
                                        is WebSocketEvent.Push -> {
                                            eventData = eventData.setError(IllegalStateException("Push event was received before confirmation event"))
                                            state = SubscribeToState.UNSUBSCRIBE
                                        }
                                    }
                                } catch (e: Throwable) {
                                    eventData = eventData.setError(e)
                                    state = SubscribeToState.UNSUBSCRIBE
                                }

                                this@channelFlow.send(eventData)
                            }
                        } catch (e: CancellationException) {
                            state = when (state) {
                                SubscribeToState.SUBSCRIBE -> SubscribeToState.SUBSCRIBE
                                SubscribeToState.CONFIRM_SUBSCRIPTION -> SubscribeToState.CONFIRM_SUBSCRIPTION
                                SubscribeToState.CONSUME_EVENTS -> SubscribeToState.UNSUBSCRIBE
                                SubscribeToState.UNSUBSCRIBE -> SubscribeToState.UNSUBSCRIBE
                                else -> SubscribeToState.EXIT
                            }
                        }
                    }
                    SubscribeToState.CONSUME_EVENTS -> {
                        try {
                            try {
                                for (msg in connection.responseChannelRegistry.get(channel)!!) {
                                    when (msg) {
                                        is WebSocketEvent.Push -> {
                                            eventData = eventData.setPayload(json.decodeFromJsonElement(payloadType, msg.data))
                                            this@channelFlow.send(eventData)
                                        }
                                        is WebSocketEvent.Error -> {
                                            throw msg.toException()
                                        }
                                        is WebSocketEvent.Response -> {
                                            throw IllegalStateException("Subscribe/Unsubscribe event can't be received during events consumption")
                                        }
                                    }
                                }
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: DisconnectedException) {
                                eventData = eventData.setError(e)
                                state = SubscribeToState.EXIT
                                this@channelFlow.send(eventData)
                            } catch (e: Throwable) {
                                eventData = eventData.setError(e)
                                state = SubscribeToState.UNSUBSCRIBE
                                this@channelFlow.send(eventData)
                            }
                        } catch (e: CancellationException) {
                            state = when (state) {
                                SubscribeToState.EXIT -> SubscribeToState.EXIT
                                else -> SubscribeToState.UNSUBSCRIBE
                            }
                        }
                    }
                    SubscribeToState.UNSUBSCRIBE -> {
                        val request = WebSocketRequest(WebSocketRequest.Method.UNSUBSCRIBE, listOf(channel), connection.generateId())
                        val internalRequest = InternalWebSocketRequest(request, connection.responseChannelRegistry.get(channel)!!)
                        state = try {
                            withContext(NonCancellable) {
                                connection.requestChannel.send(internalRequest)
                            }
                            SubscribeToState.CONFIRM_UNSUBSCRIPTION
                        } catch (e: CancellationException) {
                            SubscribeToState.CONFIRM_UNSUBSCRIPTION
                        } catch (e: DisconnectedException) {
                            SubscribeToState.EXIT
                        }
                    }
                    SubscribeToState.CONFIRM_UNSUBSCRIPTION -> {
                        try {
                            withContext(NonCancellable) {
                                try {
                                    withTimeout(1.5.minutes) {
                                        for (msg in connection.responseChannelRegistry.get(channel)!!) {
                                            when (msg) {
                                                is WebSocketEvent.Push -> {
                                                    eventData = eventData.setPayload(json.decodeFromJsonElement(payloadType, msg.data))
                                                    ignoreErrors { this@channelFlow.send(eventData) }
                                                }
                                                is WebSocketEvent.Response -> {
                                                    eventData = eventData.setSubscribed(false)
                                                    state = SubscribeToState.EXIT
                                                    if (logger.isDebugEnabled) logger.debug("Unsubscribed from channel $channel")
                                                    this@channelFlow.send(eventData)
                                                    return@withTimeout
                                                }
                                                is WebSocketEvent.Error -> {
                                                    throw msg.toException()
                                                }
                                            }
                                        }
                                    }
                                } catch (e: TimeoutCancellationException) {
                                    state = SubscribeToState.UNSUBSCRIBE
                                } catch (e: CancellationException) {
                                    throw e
                                } catch (e: DisconnectedException) {
                                    eventData = eventData.setError(e)
                                    state = SubscribeToState.EXIT
                                    this@channelFlow.send(eventData)
                                } catch (e: Throwable) {
                                    eventData = eventData.setError(e)
                                    state = SubscribeToState.UNSUBSCRIBE
                                    this@channelFlow.send(eventData)
                                }
                            }
                        } catch (e: CancellationException) {
                            state = when (state) {
                                SubscribeToState.UNSUBSCRIBE -> SubscribeToState.UNSUBSCRIBE
                                else -> SubscribeToState.EXIT
                            }
                        }
                    }
                    SubscribeToState.EXIT -> {
                        connection.responseChannelRegistry.remove(channel)?.close()
                        return@collect
                    }
                }
            }
        }
    }

    private fun subscribeToPrivateChannel() = channelFlow {
        suspend fun fetchListenKey(): Pair<String, Long> {
            val listenKeyCached = if (wsListenKeyFileChannel != null) {
                try {
                    val wsListenKeyJsonString = wsListenKeyFileChannel.readString()
                    json.decodeFromString<CachedListenKey>(wsListenKeyJsonString)
                } catch (e: Throwable) {
                    null
                }
            } else {
                null
            }

            val now = Instant.now()
            val pingTime = listenKeyCached?.createTime?.plusMillis(LISTEN_KEY_PING_INTERVAL.toLongMilliseconds())

            if (listenKeyCached == null || pingTime!!.isBefore(now)) {
                val listenKey = try {
                    logger.info("Trying to fetch listen key...")
                    val key = getListenKey()
                    logger.info("Listen key fetched")
                    key
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    logger.warn("Can't get listen key: ${e.message}")
                    throw e
                }

                if (wsListenKeyFileChannel != null) {
                    ignoreErrors {
                        val wsListenKeyJsonString = json.encodeToString(CachedListenKey(listenKey))
                        wsListenKeyFileChannel.writeString(wsListenKeyJsonString)
                    }
                }

                return Pair(listenKey, LISTEN_KEY_PING_INTERVAL.toLongMilliseconds())
            } else {
                return Pair(listenKeyCached.listenKey, pingTime.toEpochMilli() - now.toEpochMilli())
            }
        }

        suspend fun keepListenKeyAlive(listenKey: String) {
            while (isActive) {
                try {
                    logger.info("Ping listen key")
                    pingListenKey(listenKey)
                    logger.info("Pong listen key")
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    logger.warn("Can't ping listen key: ${e.message}")
                    throw e
                }

                if (wsListenKeyFileChannel != null) {
                    ignoreErrors {
                        val wsListenKeyJsonString = json.encodeToString(CachedListenKey(listenKey))
                        wsListenKeyFileChannel.writeString(wsListenKeyJsonString)
                    }
                }

                delay(LISTEN_KEY_PING_INTERVAL)
            }
        }

        while (isActive) {
            try {
                coroutineScope {
                    val (listenKey, waitTillNextPoll) = fetchListenKey()

                    launch(start = CoroutineStart.UNDISPATCHED) { delay(waitTillNextPoll); keepListenKeyAlive(listenKey) }

                    subscribeTo(listenKey, JsonElement.serializer()).collect { eventData ->
                        if (eventData.payload == null) {
                            send(eventData.newPayload())
                            return@collect
                        }

                        val eventType = eventData.payload.jsonObject["e"]?.jsonPrimitive?.contentOrNull

                        val accountEventSerializer = when (eventType) {
                            "listenKeyExpired" -> AccountEvent.ListenKeyExpiredEvent.serializer()
                            "MARGIN_CALL" -> AccountEvent.MarginCallEvent.serializer()
                            "ACCOUNT_UPDATE" -> AccountEvent.AccountUpdateEvent.serializer()
                            "ORDER_TRADE_UPDATE" -> AccountEvent.OrderTradeUpdateEvent.serializer()
                            null -> {
                                logger.warn("Event type is null in private channel ${eventData.payload}")
                                null
                            }
                            else -> {
                                logger.debug("Not recognized event received in private channel ${eventData.payload}")
                                null
                            }
                        } ?: return@collect

                        val accountEvent = try {
                            json.decodeFromJsonElement(accountEventSerializer, eventData.payload)
                        } catch (e: Throwable) {
                            logger.error("Can't parse json: ${e.message} ${eventData.payload}")
                            return@collect
                        }

                        send(eventData.newPayload(accountEvent))
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                logger.warn("Subscription to private channel was interrupted: ${e.message}")
            }
        }
    }

    private fun <T> cacheStream(channel: String, subscribe: (String) -> Flow<T>): Flow<T> {
        @Suppress("UNCHECKED_CAST")
        return streamCache.getOrPut(channel) {
            subscribe(channel)
                .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
        } as Flow<T>
    }
    //endregion

    //region Exceptions
    class Exception(val code: Long, val description: String) : Throwable("$code: $description", null, true, false)

    class DisconnectedException(override val cause: Throwable? = null) : Throwable("WebSocket connection was closed", cause, true, false)

    enum class Error(val code: Long, val msg: String) {
        UNKNOWN(1000, "An unknown error occurred while processing the request."),
        DISCONNECTED(-1001, "Internal error; unable to process your request. Please try again."),
        UNAUTHORIZED(-1002, "You are not authorized to execute this request."),
        TOO_MANY_REQUESTS(-1003, "Too many requests queued."),
        UNEXPECTED_RESP(-1006, "An unexpected response was received from the message bus. Execution status unknown."),
        TIMEOUT(-1007, "Timeout waiting for response from backend server. Send status unknown; execution status unknown."),
        UNKNOWN_ORDER_COMPOSITION(-1014, "Unsupported order combination."),
        TOO_MANY_ORDERS(-1015, "Too many new orders."),
        SERVICE_SHUTTING_DOWN(-1016, "This service is no longer available."),
        UNSUPPORTED_OPERATION(-1020, "This operation is not supported."),
        INVALID_TIMESTAMP(-1021, "Timestamp for this request is outside of the recvWindow."),
        INVALID_SIGNATURE(-1022, "Signature for this request is not valid."),
        NOT_FOUND_AUTHENTICATED_OR_AUTHORIZED(-1099, "Not found, authenticated, or authorized"),
        ILLEGAL_CHARS(-1100, "Illegal characters found in a parameter."),
        TOO_MANY_PARAMETERS(-1101, "Too many parameters sent for this endpoint."),
        MANDATORY_PARAM_EMPTY_OR_MALFORMED(-1102, "A mandatory parameter was not sent, was empty/null, or malformed."),
        UNKNOWN_PARAM(-1103, "An unknown parameter was sent."),
        UNREAD_PARAMETERS(-1104, "Not all sent parameters were read."),
        PARAM_EMPTY(-1105, "A parameter was empty."),
        PARAM_NOT_REQUIRED(-1106, "A parameter was sent when not required."),
        BAD_PRECISION(-1111, "Precision is over the maximum defined for this asset."),
        NO_DEPTH(-1112, "No orders on book for symbol."),
        TIF_NOT_REQUIRED(-1114, "TimeInForce parameter sent when not required."),
        INVALID_TIF(-1115, "Invalid timeInForce."),
        INVALID_ORDER_TYPE(-1116, "Invalid orderType."),
        INVALID_SIDE(-1117, "Invalid side."),
        EMPTY_NEW_CL_ORD_ID(-1118, "New client order ID was empty."),
        EMPTY_ORG_CL_ORD_ID(-1119, "Original client order ID was empty."),
        BAD_INTERVAL(-1120, "Invalid interval."),
        BAD_SYMBOL(-1121, "Invalid symbol."),
        INVALID_LISTEN_KEY(-1125, "This listenKey does not exist."),
        MORE_THAN_XX_HOURS(-1127, "Lookup interval is too big."),
        OPTIONAL_PARAMS_BAD_COMBO(-1128, "Combination of optional parameters invalid."),
        INVALID_PARAMETER(-1130, "Invalid data sent for a parameter."),
        BAD_RECV_WINDOW(-1131, "recvWindow must be less than 60000"),
        NEW_ORDER_REJECTED(-2010, "New order rejected"),
        CANCEL_REJECTED(-2011, "Cancel rejected"),
        NO_SUCH_ORDER(-2013, "Order does not exist."),
        BAD_API_KEY_FMT(-2014, "API-key format invalid."),
        REJECTED_MBX_KEY(-2015, "Invalid API-key, IP, or permissions for action."),
        NO_TRADING_WINDOW(-2016, "No trading window could be found for the symbol. Try ticker/24hrs instead."),
        PAIR_ADMIN_BAN_TRADE(-3021, "Margin account are not allowed to trade this trading pair."),
        ACCOUNT_BAN_TRADE(-3022, "You account's trading is banned."),
        WARNING_MARGIN_LEVEL(-3023, "You can't transfer out/place order under current margin level."),
        FEW_LIABILITY_LEFT(-3024, "The unpaid debt is too small after this repayment."),
        INVALID_EFFECTIVE_TIME(-3025, "Your input date is invalid."),
        VALIDATION_FAILED(-3026, "Your input param is invalid."),
        NOT_VALID_MARGIN_ASSET(-3027, "Not a valid margin asset."),
        NOT_VALID_MARGIN_PAIR(-3028, "Not a valid margin pair."),
        TRANSFER_FAILED(-3029, "Transfer failed."),
        ACCOUNT_BAN_REPAY(-3036, "This account is not allowed to repay."),
        PNL_CLEARING(-3037, "PNL is clearing. Wait a second."),
        LISTEN_KEY_NOT_FOUND(-3038, "Listen key not found."),
        PRICE_INDEX_NOT_FOUND(-3042, "PriceIndex not available for this margin pair."),
        NOT_WHITELIST_USER(-3999, "This function is only available for invited users."),
        CAPITAL_INVALID(-4001, "Invalid operation."),
        CAPITAL_IG(-4002, "Invalid get."),
        CAPITAL_IEV(-4003, "Your input email is invalid."),
        CAPITAL_UA(-4004, "You don't login or auth."),
        CAPAITAL_TOO_MANY_REQUEST(-4005, "Too many new requests."),
        CAPITAL_ONLY_SUPPORT_PRIMARY_ACCOUNT(-4006, "Support main account only."),
        CAPITAL_ADDRESS_VERIFICATION_NOT_PASS(-4007, "Address validation is not passed."),
        CAPITAL_ADDRESS_TAG_VERIFICATION_NOT_PASS(-4008, "Address tag validation is not passed."),
        ASSET_NOT_SUPPORTED(-5011, "This asset is not supported."),
        DAILY_PRODUCT_NOT_EXIST(-6001, "Daily product not exists."),
        DAILY_PRODUCT_NOT_ACCESSIBLE(-6003, "Product not exist or you don't have permission"),
        DAILY_PRODUCT_NOT_PURCHASABLE(-6004, "Product not in purchase status"),
        DAILY_LOWER_THAN_MIN_PURCHASE_LIMIT(-6005, "Smaller than min purchase limit"),
        DAILY_REDEEM_AMOUNT_ERROR(-6006, "Redeem amount error"),
        DAILY_REDEEM_TIME_ERROR(-6007, "Not in redeem time"),
        DAILY_PRODUCT_NOT_REDEEMABLE(-6008, "Product not in redeem status"),
        REQUEST_FREQUENCY_TOO_HIGH(-6009, "Request frequency too high"),
        EXCEEDED_USER_PURCHASE_LIMIT(-6011, "Exceeding the maximum num allowed to purchase per user"),
        BALANCE_NOT_ENOUGH(-6012, "Balance not enough"),
        PURCHASING_FAILED(-6013, "Purchasing failed"),
        UPDATE_FAILED(-6014, "Exceed up-limit allowed to purchased"),
        EMPTY_REQUEST_BODY(-6015, "Empty request body"),
        PARAMS_ERR(-6016, "Parameter err"),
        NOT_IN_WHITELIST(-6017, "Not in whitelist"),
        ASSET_NOT_ENOUGH(-6018, "Asset not enough"),
        PENDING(-6019, "Need confirm"),
    }
    //endregion

    //region Private Extensions
    private fun Map<String, String>.toQueryString() = asSequence().map { "${it.key}=${it.value}" }.joinToString("&")
    private fun String.appendToQueryString(key: String, value: String) = "${if (isBlank()) "" else "$this&"}$key=$value"
    private fun String.appendToUri() = if (isBlank()) "" else "?$this"

    private fun Error.toException() = Exception(code, msg)
    private fun WebSocketEvent.Error.toException() = Exception(code, msg)
    //endregion

    //region Request Limiters
    private class SimpleRequestLimiter(private val allowedRequests: Int, private val perIntervalMs: Long) {
        private var reqCountFromFirst = 0L
        private var firstReqExecTime = 0L
        private var lastReqExecTime = 0L

        fun waitMs(): Long {
            val currReqTime = System.currentTimeMillis()

            if (currReqTime - lastReqExecTime > perIntervalMs) {
                reqCountFromFirst = 0
                firstReqExecTime = currReqTime
                lastReqExecTime = currReqTime
            } else {
                val offset = perIntervalMs * (reqCountFromFirst / allowedRequests)
                lastReqExecTime = firstReqExecTime + offset
            }

            reqCountFromFirst += 1

            return run {
                val d = lastReqExecTime - currReqTime
                if (d <= 0) 0 else d
            }
        }
    }
    //endregion

    companion object {
        //region Constants
        private const val API_URL_MAIN_NET = "https://fapi.binance.com"
        private const val API_URL_TEST_NET = "https://testnet.binancefuture.com"
        private const val API_STREAM_URL_MAIN_NET = "wss://fstream.binance.com/stream"
        private const val API_STREAM_URL_TEST_NET = "wss://stream.binancefuture.com/stream"

        private const val API_KEY_HEADER = "X-MBX-APIKEY"
        private const val SIGNATURE = "signature"

        private val LISTEN_KEY_PING_INTERVAL = 45.minutes

        private val FILE_OPTIONS = arrayOf(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
        //endregion
    }
}
