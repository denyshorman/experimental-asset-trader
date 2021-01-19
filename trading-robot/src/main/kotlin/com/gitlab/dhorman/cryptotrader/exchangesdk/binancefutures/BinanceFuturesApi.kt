package com.gitlab.dhorman.cryptotrader.exchangesdk.binancefutures

import com.gitlab.dhorman.cryptotrader.util.*
import com.gitlab.dhorman.cryptotrader.util.limiter.SimpleRequestLimiter
import com.gitlab.dhorman.cryptotrader.util.serializer.*
import com.gitlab.dhorman.cryptotrader.util.signer.HmacSha256Signer
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
import java.math.BigDecimal
import java.net.URI
import java.nio.channels.ClosedChannelException
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.HashMap
import kotlin.reflect.KClass
import kotlin.time.minutes
import kotlin.time.seconds

class BinanceFuturesApi(
    apiKey: String,
    apiSecret: String,
    apiNet: ApiNet,
) {
    private val json: Json = Json {
        ignoreUnknownKeys = true
    }

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("BinanceFuturesApi"))

    private val webSocketConnector = WebSocketConnector(apiNet, this, scope, json)
    private val httpConnector = HttpConnector(apiNet, apiKey, apiSecret, json)

    //region Market Data API
    suspend fun ping() {
        return httpConnector.callApi("/fapi/v1/ping", HttpMethod.GET, emptyMap(), false, false, serializer())
    }

    suspend fun getCurrentServerTime(): ServerTimeResp {
        return httpConnector.callApi("/fapi/v1/time", HttpMethod.GET, emptyMap(), false, false, serializer())
    }

    suspend fun getExchangeInfo(): ExchangeInfo {
        return httpConnector.callApi("/fapi/v1/exchangeInfo", HttpMethod.GET, emptyMap(), false, false, serializer())
    }
    //endregion

    //region Account/Trades API
    suspend fun getCommissionRate(symbol: String, timestamp: Instant = Instant.now(), recvWindow: Long? = null): CommissionRate {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/commissionRate", HttpMethod.GET, params, true, true, serializer())
    }

    suspend fun getCurrentPositionMode(recvWindow: Long? = null, timestamp: Instant = Instant.now()): PositionMode {
        val params = buildMap<String, String> {
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/positionSide/dual", HttpMethod.GET, params, true, true, serializer())
    }

    suspend fun placeNewOrder(
        symbol: String,
        side: OrderSide,
        type: OrderType,
        positionSide: PositionSide? = null,
        timeInForce: TimeInForce? = null,
        quantity: BigDecimal? = null,
        reduceOnly: Boolean? = null,
        price: BigDecimal? = null,
        newClientOrderId: String? = null,
        stopPrice: BigDecimal? = null,
        closePosition: Boolean? = null,
        activationPrice: BigDecimal? = null,
        callbackRate: BigDecimal? = null,
        workingType: WorkingType? = null,
        priceProtect: Boolean? = null,
        newOrderRespType: ResponseType? = null,
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): Order {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("side", side.toString())
            put("type", type.toString())
            put("timestamp", timestamp.toEpochMilli().toString())
            if (positionSide != null) put("positionSide", positionSide.toString())
            if (timeInForce != null) put("timeInForce", timeInForce.id)
            if (quantity != null) put("quantity", quantity.toString())
            if (reduceOnly != null) put("reduceOnly", reduceOnly.toString())
            if (price != null) put("price", price.toString())
            if (newClientOrderId != null) put("newClientOrderId", newClientOrderId)
            if (stopPrice != null) put("stopPrice", stopPrice.toString())
            if (closePosition != null) put("closePosition", closePosition.toString())
            if (activationPrice != null) put("activationPrice", activationPrice.toString())
            if (callbackRate != null) put("callbackRate", callbackRate.toString())
            if (workingType != null) put("workingType", workingType.toString())
            if (priceProtect != null) put("priceProtect", priceProtect.toString())
            if (newOrderRespType != null) put("newOrderRespType", newOrderRespType.toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/order", HttpMethod.POST, params, true, true, serializer())
    }

    suspend fun cancelOrder(
        symbol: String,
        orderId: Long? = null,
        origClientOrderId: String? = null,
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): Order {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("timestamp", timestamp.toEpochMilli().toString())
            if (orderId != null) put("orderId", orderId.toString())
            if (origClientOrderId != null) put("origClientOrderId", origClientOrderId)
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/order", HttpMethod.DELETE, params, true, true, serializer())
    }

    suspend fun getAllOrders(
        symbol: String,
        orderId: Long? = null,
        startTime: Instant? = null,
        endTime: Instant? = null,
        limit: Int? = null,
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): List<HistoryOrder> {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("timestamp", timestamp.toEpochMilli().toString())
            if (orderId != null) put("orderId", orderId.toString())
            if (startTime != null) put("startTime", startTime.toEpochMilli().toString())
            if (endTime != null) put("endTime", endTime.toEpochMilli().toString())
            if (limit != null) put("limit", limit.toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/allOrders", HttpMethod.GET, params, true, true, serializer())
    }

    suspend fun getAllOpenOrders(
        symbol: String? = null,
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): List<HistoryOrder> {
        val params = buildMap<String, String> {
            put("timestamp", timestamp.toEpochMilli().toString())
            if (symbol != null) put("symbol", symbol)
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/openOrders", HttpMethod.GET, params, true, true, serializer())
    }

    suspend fun getCurrentPositions(
        symbol: String? = null,
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): List<Position> {
        val params = buildMap<String, String> {
            put("timestamp", timestamp.toEpochMilli().toString())
            if (symbol != null) put("symbol", symbol)
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v2/positionRisk", HttpMethod.GET, params, true, true, serializer())
    }

    suspend fun changeInitialLeverage(
        symbol: String,
        leverage: Int,
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): LeverageChangeResp {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("leverage", leverage.toString())
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/leverage", HttpMethod.POST, params, true, true, serializer())

    }

    suspend fun changeMarginType(
        symbol: String,
        marginType: MarginType,
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): JsonElement {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("marginType", marginType.toString())
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v1/marginType", HttpMethod.POST, params, true, true, serializer())
    }

    suspend fun getAccountBalance(
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): List<AccountBalance> {
        val params = buildMap<String, String> {
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v2/balance", HttpMethod.GET, params, true, true, serializer())
    }

    suspend fun getAccountInfo(
        recvWindow: Long? = null,
        timestamp: Instant = Instant.now(),
    ): AccountInfo {
        val params = buildMap<String, String> {
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return httpConnector.callApi("/fapi/v2/account", HttpMethod.GET, params, true, true, serializer())
    }
    //endregion

    //region User Data Streams API
    suspend fun getListenKey(): String {
        val resp = httpConnector.callApi("/fapi/v1/listenKey", HttpMethod.POST, emptyMap(), true, false, serializer<ListenKey>())
        return resp.listenKey
    }

    suspend fun pingListenKey(listenKey: String) {
        val params = mapOf(Pair("listenKey", listenKey))
        httpConnector.callApi("/fapi/v1/listenKey", HttpMethod.PUT, params, true, false, serializer<Unit>())
    }

    suspend fun deleteListenKey(listenKey: String) {
        val params = mapOf(Pair("listenKey", listenKey))
        httpConnector.callApi("/fapi/v1/listenKey", HttpMethod.DELETE, params, true, false, serializer<Unit>())
    }
    //endregion

    //region Market Streams
    fun aggregateTradeStream(symbol: String): Flow<EventData<AggregateTradeEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@aggTrade", serializer())
    }

    fun tradeStream(symbol: String): Flow<EventData<TradeEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@trade", serializer())
    }

    fun markPriceStream(symbol: String, updateSpeed: MarkPriceUpdateSpeed? = null): Flow<EventData<MarkPriceEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeSec}s"
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@markPrice$updateSpeedStr", serializer())
    }

    fun allMarketsMarkPriceStream(updateSpeed: MarkPriceUpdateSpeed? = null): Flow<EventData<List<MarkPriceEvent>>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeSec}s"
        return webSocketConnector.subscribeTo("!markPrice@arr$updateSpeedStr", serializer())
    }

    fun candlestickStream(symbol: String, interval: CandleStickInterval): Flow<EventData<CandlestickEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@kline_${interval.id}", serializer())
    }

    fun continuousContractCandlestickStream(symbol: String, contractType: ContractType, interval: CandleStickInterval): Flow<EventData<ContinuousCandlestickEvent>> {
        val channel = "${symbol.toLowerCase()}_${contractType.toString().toLowerCase()}@continuousKline_${interval.id}"
        return webSocketConnector.subscribeTo(channel, serializer())
    }

    fun individualSymbolMiniTickerStream(symbol: String): Flow<EventData<MiniTickerEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@miniTicker", serializer())
    }

    val allMarketMiniTickersStream: Flow<EventData<List<MiniTickerEvent>>> = run {
        webSocketConnector.subscribeTo("!miniTicker@arr", serializer())
    }

    fun individualSymbolTickerStream(symbol: String): Flow<EventData<TickerEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@ticker", serializer())
    }

    val allMarketTickersStream: Flow<EventData<List<TickerEvent>>> = run {
        webSocketConnector.subscribeTo("!ticker@arr", serializer())
    }

    fun individualSymbolBookTickerStream(symbol: String): Flow<EventData<BookTickerEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@bookTicker", serializer())
    }

    val allBookTickerStream: Flow<EventData<BookTickerEvent>> = run {
        webSocketConnector.subscribeTo("!bookTicker", serializer())
    }

    fun liquidationOrderStream(symbol: String): Flow<EventData<LiquidationOrderEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@forceOrder", serializer())
    }

    val allMarketLiquidationOrderStream: Flow<EventData<LiquidationOrderEvent>> = run {
        webSocketConnector.subscribeTo("!forceOrder@arr", serializer())
    }

    fun partialBookDepthStream(
        symbol: String,
        level: PartialBookDepthEvent.Level,
        updateSpeed: BookUpdateSpeed? = null,
    ): Flow<EventData<PartialBookDepthEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeMs}ms"
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@depth${level.id}$updateSpeedStr", serializer())
    }

    fun diffDepthStream(
        symbol: String,
        updateSpeed: BookUpdateSpeed? = null,
    ): Flow<EventData<DiffDepthEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeMs}ms"
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@depth$updateSpeedStr", serializer())
    }

    fun compositeIndexStream(symbol: String): Flow<EventData<CompositeIndexEvent>> {
        return webSocketConnector.subscribeTo("${symbol.toLowerCase()}@compositeIndex", serializer())
    }
    //endregion

    //region User Data Streams
    val accountStream: Flow<EventData<AccountEvent>> = run {
        webSocketConnector.subscribeToPrivateChannel()
    }
    //endregion

    //region Public Models
    enum class ApiNet {
        Main,
        Test,
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
    enum class TimeInForce(val id: String) {
        @SerialName("GTC")
        GOOD_TILL_CANCEL("GTC"),

        @SerialName("GTE_GTC")
        GOOD_TILL_EXPIRED_OR_CANCELED("GTE_GTC"),

        @SerialName("IOC")
        IMMEDIATE_OR_CANCEL("IOC"),

        @SerialName("FOK")
        FILL_OR_KILL("FOK"),

        @SerialName("GTX")
        POST_ONLY("GTX"),
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
    enum class MarginType {
        @SerialName("crossed")
        CROSSED,

        @SerialName("isolated")
        ISOLATED,
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

        val filtersIndexed: Map<KClass<out ExchangeFilter>, ExchangeFilter> by lazy(LazyThreadSafetyMode.PUBLICATION) {
            exchangeFilters.asSequence().map { it::class to it }.toMap()
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
            val filtersIndexed: Map<KClass<out ExchangeFilter>, ExchangeFilter> by lazy(LazyThreadSafetyMode.PUBLICATION) {
                filters.asSequence().map { it::class to it }.toMap()
            }

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

    @Serializable
    data class ListenKey(
        val listenKey: String,
    )

    @Serializable
    data class CommissionRate(
        val symbol: String,
        @Serializable(BigDecimalAsStringSerializer::class) val makerCommissionRate: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val takerCommissionRate: BigDecimal,
    )

    @Serializable
    data class PositionMode(
        val dualSidePosition: Boolean,
    )

    @Serializable
    data class Order(
        val orderId: Long,
        val clientOrderId: String,
        val symbol: String,
        val status: OrderStatus,
        val side: OrderSide,
        val positionSide: PositionSide,
        val type: OrderType,
        val origType: OrderType,
        val workingType: WorkingType,
        val timeInForce: TimeInForce,
        val reduceOnly: Boolean,
        val closePosition: Boolean,
        val priceProtect: Boolean,
        @Serializable(BigDecimalAsStringSerializer::class) val price: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val stopPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val avgPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val origQty: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val executedQty: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val cumQty: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val cumQuote: BigDecimal,
        @Serializable(InstantAsLongMillisSerializer::class) val updateTime: Instant,
    )

    @Serializable
    data class HistoryOrder(
        val orderId: Long,
        val clientOrderId: String,
        val symbol: String,
        val status: OrderStatus,
        val side: OrderSide,
        val positionSide: PositionSide,
        val type: OrderType,
        val origType: OrderType,
        val workingType: WorkingType,
        val timeInForce: TimeInForce,
        val reduceOnly: Boolean,
        val closePosition: Boolean,
        val priceProtect: Boolean,
        @Serializable(BigDecimalAsStringSerializer::class) val price: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val stopPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val avgPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val origQty: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val executedQty: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val cumQuote: BigDecimal,
        @Serializable(InstantAsLongMillisSerializer::class) val time: Instant,
        @Serializable(InstantAsLongMillisSerializer::class) val updateTime: Instant,
    )

    @Serializable
    data class Position(
        val symbol: String,
        val marginType: MarginType,
        @SerialName("positionSide") val side: PositionSide,
        @Serializable(BigDecimalAsStringSerializer::class) @SerialName("positionAmt") val amount: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val entryPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val markPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val unRealizedProfit: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val liquidationPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val leverage: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val notional: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val maxNotionalValue: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val isolatedMargin: BigDecimal,
        @Serializable(BooleanAsStringSerializer::class) val isAutoAddMargin: Boolean,
    )

    @Serializable
    data class LeverageChangeResp(
        val symbol: String,
        val leverage: Int,
        @Serializable(BigDecimalAsStringSerializer::class) val maxNotionalValue: BigDecimal,
    )

    @Serializable
    data class AccountBalance(
        val accountAlias: String,
        val asset: String,
        @Serializable(BigDecimalAsStringSerializer::class) val balance: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val crossWalletBalance: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val crossUnPnl: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val availableBalance: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val maxWithdrawAmount: BigDecimal,
    )

    @Serializable
    data class AccountInfo(
        val feeTier: Int,
        val canTrade: Boolean,
        val canDeposit: Boolean,
        val canWithdraw: Boolean,
        val updateTime: Long,
        @Serializable(BigDecimalAsStringSerializer::class) val totalInitialMargin: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalMaintMargin: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalWalletBalance: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalUnrealizedProfit: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalMarginBalance: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalPositionInitialMargin: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalOpenOrderInitialMargin: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalCrossWalletBalance: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val totalCrossUnPnl: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val availableBalance: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val maxWithdrawAmount: BigDecimal,
        val assets: List<Asset>,
        val positions: List<Position>,
    ) {
        @Serializable
        data class Asset(
            val asset: String,
            @Serializable(BigDecimalAsStringSerializer::class) val walletBalance: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val availableBalance: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val marginBalance: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val crossWalletBalance: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val initialMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) @SerialName("maintMargin") val maintenanceMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val positionInitialMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val openOrderInitialMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val unrealizedProfit: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val crossUnPnl: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val maxWithdrawAmount: BigDecimal,
        )

        @Serializable
        data class Position(
            val symbol: String,
            @Serializable(BigDecimalAsStringSerializer::class) val initialMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val positionInitialMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val openOrderInitialMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) @SerialName("maintMargin") val maintenanceMargin: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val unrealizedProfit: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val leverage: BigDecimal,
            val isolated: Boolean,
            @Serializable(BigDecimalAsStringSerializer::class) val entryPrice: BigDecimal,
            val positionSide: PositionSide,
            @Serializable(BigDecimalAsStringSerializer::class) val positionAmt: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val notional: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val maxNotional: BigDecimal,
        )
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
                @SerialName("m") val eventReasonType: EventReasonType,
                @SerialName("B") val balances: List<Balance>,
                @SerialName("P") val positions: List<Position>,
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
                data class Balance(
                    @SerialName("a") val asset: String,
                    @SerialName("wb") @Serializable(BigDecimalAsStringSerializer::class) val walletBalance: BigDecimal,
                    @SerialName("cw") @Serializable(BigDecimalAsStringSerializer::class) val crossWalletBalance: BigDecimal,
                )

                @Serializable
                data class Position(
                    @SerialName("s") val symbol: String,
                    @SerialName("mt") val marginType: MarginType,
                    @SerialName("ps") val positionSide: PositionSide,
                    @SerialName("pa") @Serializable(BigDecimalAsStringSerializer::class) val positionAmount: BigDecimal,
                    @SerialName("ep") @Serializable(BigDecimalAsStringSerializer::class) val entryPrice: BigDecimal,
                    @SerialName("cr") @Serializable(BigDecimalAsStringSerializer::class) val preFeeAccumulatedRealized: BigDecimal,
                    @SerialName("up") @Serializable(BigDecimalAsStringSerializer::class) val unrealizedPnL: BigDecimal,
                    @SerialName("iw") @Serializable(BigDecimalAsStringSerializer::class) val isolatedWallet: BigDecimal? = null,
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
                @SerialName("t") val tradeId: Long,
                @SerialName("S") val side: OrderSide,
                @SerialName("ps") val positionSide: PositionSide,
                @SerialName("X") val orderStatus: OrderStatus,
                @SerialName("o") val orderType: OrderType,
                @SerialName("x") val executionType: ExecutionType,
                @SerialName("ot") val originalOrderType: OrderType,
                @SerialName("wt") val stopPriceWorkingType: WorkingType,
                @SerialName("f") val timeInForce: TimeInForce,
                @SerialName("m") val makerSide: Boolean,
                @SerialName("R") val reduceOnly: Boolean,
                @SerialName("cp") val conditionalOrder: Boolean,
                @SerialName("N") val commissionAsset: String? = null,
                @SerialName("n") @Serializable(BigDecimalAsStringSerializer::class) val commission: BigDecimal? = null,
                @SerialName("p") @Serializable(BigDecimalAsStringSerializer::class) val originalPrice: BigDecimal,
                @SerialName("ap") @Serializable(BigDecimalAsStringSerializer::class) val averagePrice: BigDecimal,
                @SerialName("AP") @Serializable(BigDecimalAsStringSerializer::class) val activationPrice: BigDecimal? = null,
                @SerialName("sp") @Serializable(BigDecimalAsStringSerializer::class) val stopPrice: BigDecimal? = null,
                @SerialName("L") @Serializable(BigDecimalAsStringSerializer::class) val lastFilledPrice: BigDecimal,
                @SerialName("q") @Serializable(BigDecimalAsStringSerializer::class) val originalQuantity: BigDecimal,
                @SerialName("l") @Serializable(BigDecimalAsStringSerializer::class) val orderLastFilledQuantity: BigDecimal,
                @SerialName("z") @Serializable(BigDecimalAsStringSerializer::class) val orderFilledAccumulatedQuantity: BigDecimal,
                @SerialName("b") @Serializable(BigDecimalAsStringSerializer::class) val bidsNotional: BigDecimal,
                @SerialName("a") @Serializable(BigDecimalAsStringSerializer::class) val askNotional: BigDecimal,
                @SerialName("cr") @Serializable(BigDecimalAsStringSerializer::class) val callbackRate: BigDecimal? = null,
                @SerialName("rp") @Serializable(BigDecimalAsStringSerializer::class) val realizedProfit: BigDecimal,
                @SerialName("T") @Serializable(InstantAsLongMillisSerializer::class) val orderTradeTime: Instant,
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

    //region Exceptions
    class Exception(val code: Long, val description: String) : Throwable("$code: $description", null, true, false)

    class DisconnectedException(override val cause: Throwable? = null) : Throwable("WebSocket connection was closed", cause, true, false)

    enum class Error(val code: Long, val msg: String) {
        // TODO: Add errors
    }
    //endregion

    private class WebSocketConnector(
        apiNet: ApiNet,
        val binanceFuturesApi: BinanceFuturesApi,
        val scope: CoroutineScope,
        val json: Json,
    ) {
        private val apiUrlStream = when (apiNet) {
            ApiNet.Main -> API_STREAM_URL_MAIN_NET
            ApiNet.Test -> API_STREAM_URL_TEST_NET
        }

        private val streamCache = ConcurrentHashMap<String, Flow<EventData<*>>>()

        private val webSocketClient = springWebsocketClient(
            connectTimeoutMs = 10000,
            readTimeoutMs = 5000,
            writeTimeoutMs = 5000,
            maxFramePayloadLength = 65536 * 4,
        )

        private val connection: Flow<ConnectionData> = run {
            channelFlow<ConnectionData> connection@{
                logger.debug("Starting WebSocket connection channel")

                while (isActive) {
                    logger.debug("Establishing connection...")

                    try {
                        var connectionData: ConnectionData? = null

                        val session = webSocketClient.execute(URI.create(apiUrlStream)) { session ->
                            mono(Dispatchers.Unconfined) {
                                logger.info("Connection established")

                                coroutineScope {
                                    val wsMsgCounter = AtomicLong(0)
                                    val wsMsgReceiver = Channel<WebSocketMessage>(Channel.RENDEZVOUS)
                                    val requestResponses = ConcurrentHashMap<Long, List<Channel<WebSocketInboundMessage>>>()
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
                                                        json.decodeFromString<WebSocketInboundMessage.Push>(payloadJsonString)
                                                    } catch (e: SerializationException) {
                                                        try {
                                                            json.decodeFromString<WebSocketInboundMessage.Response>(payloadJsonString)
                                                        } catch (e: SerializationException) {
                                                            val error = json.decodeFromString<WebSocketInboundMessage.Error>(payloadJsonString)
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
                                                    is WebSocketInboundMessage.Push -> ignoreErrors { connectionData!!.inboundChannelRegistry.get(event.stream)?.send(event) }
                                                    is WebSocketInboundMessage.Response -> requestResponses.remove(event.id)?.forEach { ignoreErrors { it.send(event) } }
                                                    is WebSocketInboundMessage.Error -> requestResponses.remove(event.id)?.forEach { ignoreErrors { it.send(event) } }
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
                                        val reqQueue = LinkedList<InternalWebSocketOutboundMessage>()

                                        while (isActive) {
                                            reqQueue.add(connectionData!!.outboundChannel.receive())

                                            withTimeoutOrNull(250) {
                                                for (req in connectionData!!.outboundChannel) reqQueue.add(req)
                                            }

                                            val groupedRequests = reqQueue
                                                .groupBy { it.outboundMessage.method }
                                                .mapValues { (method, requests) ->
                                                    when (method) {
                                                        WebSocketOutboundMessage.Method.SUBSCRIBE, WebSocketOutboundMessage.Method.UNSUBSCRIBE -> {
                                                            val channels = LinkedList<String>()
                                                            val reqResponses = LinkedList<Channel<WebSocketInboundMessage>>()
                                                            for (req in requests) {
                                                                channels.addAll(req.outboundMessage.params)
                                                                reqResponses.add(req.inboundChannel)
                                                            }
                                                            val newReq = WebSocketOutboundMessage(method, channels, connectionData!!.generateId())
                                                            requestResponses[newReq.id] = reqResponses
                                                            listOf(newReq)
                                                        }
                                                        else -> {
                                                            val newRequests = LinkedList<WebSocketOutboundMessage>()
                                                            for (request in requests) {
                                                                newRequests.add(request.outboundMessage)
                                                                requestResponses[request.outboundMessage.id] = listOf(request.inboundChannel)
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
                        logger.info("Connection closed")
                    }
                }

                logger.debug("Closing WebSocket connection channel")
            }
                .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 1)
                .filter { !it.isClosed() }
        }

        fun <T> subscribeTo(
            channel: String,
            payloadType: DeserializationStrategy<out T>,
        ): Flow<EventData<T>> {
            @Suppress("UNCHECKED_CAST")
            return streamCache.getOrPut(channel) {
                subscribeToImpl(channel, payloadType)
                    .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 1)
                    .transformFirst { if (it.subscribed) emit(it.subscribed()) }
            } as Flow<EventData<T>>
        }

        private fun <T> subscribeToImpl(
            channel: String,
            payloadType: DeserializationStrategy<out T>,
        ): Flow<EventData<T>> = channelFlow {
            connection.conflate().collect { connection ->
                var state = SubscriptionState.INIT
                var eventData = EventData<T>()

                while (true) {
                    when (state) {
                        SubscriptionState.INIT -> {
                            try {
                                withContext(NonCancellable) {
                                    val registered = connection.inboundChannelRegistry.register(channel, Channel(64))
                                    state = if (registered) SubscriptionState.SUBSCRIBE else SubscriptionState.EXIT
                                }
                            } catch (e: CancellationException) {
                                state = SubscriptionState.EXIT
                            }
                        }
                        SubscriptionState.SUBSCRIBE -> {
                            val request = WebSocketOutboundMessage(WebSocketOutboundMessage.Method.SUBSCRIBE, listOf(channel), connection.generateId())
                            val internalRequest = InternalWebSocketOutboundMessage(request, connection.inboundChannelRegistry.get(channel)!!)
                            state = try {
                                withContext(NonCancellable) {
                                    connection.outboundChannel.send(internalRequest)
                                }
                                SubscriptionState.CONFIRM_SUBSCRIPTION
                            } catch (e: CancellationException) {
                                SubscriptionState.CONFIRM_SUBSCRIPTION
                            } catch (e: DisconnectedException) {
                                SubscriptionState.EXIT
                            }
                        }
                        SubscriptionState.CONFIRM_SUBSCRIPTION -> {
                            try {
                                withContext(NonCancellable) {
                                    val msg = try {
                                        withTimeout(10.seconds) {
                                            connection.inboundChannelRegistry.get(channel)!!.receive()
                                        }
                                    } catch (e: TimeoutCancellationException) {
                                        eventData = eventData.setError(Exception("Subscribe confirmation has not been received within specified timeout"))
                                        state = SubscriptionState.SUBSCRIBE
                                        this@channelFlow.send(eventData)
                                        return@withContext
                                    } catch (e: DisconnectedException) {
                                        eventData = eventData.setError(e)
                                        state = SubscriptionState.EXIT
                                        this@channelFlow.send(eventData)
                                        return@withContext
                                    }

                                    try {
                                        when (msg) {
                                            is WebSocketInboundMessage.Response -> {
                                                eventData = eventData.setSubscribed(true)
                                                state = SubscriptionState.CONSUME_EVENTS
                                                if (logger.isDebugEnabled) logger.debug("Subscribed to channel $channel")
                                            }
                                            is WebSocketInboundMessage.Error -> {
                                                eventData = eventData.setError(msg.toException())
                                                state = SubscriptionState.SUBSCRIBE
                                            }
                                            is WebSocketInboundMessage.Push -> {
                                                eventData = eventData.setError(IllegalStateException("Push event was received before confirmation event"))
                                                state = SubscriptionState.UNSUBSCRIBE
                                            }
                                        }
                                    } catch (e: Throwable) {
                                        eventData = eventData.setError(e)
                                        state = SubscriptionState.UNSUBSCRIBE
                                    }

                                    this@channelFlow.send(eventData)
                                }
                            } catch (e: CancellationException) {
                                state = when (state) {
                                    SubscriptionState.SUBSCRIBE -> SubscriptionState.SUBSCRIBE
                                    SubscriptionState.CONFIRM_SUBSCRIPTION -> SubscriptionState.CONFIRM_SUBSCRIPTION
                                    SubscriptionState.CONSUME_EVENTS -> SubscriptionState.UNSUBSCRIBE
                                    SubscriptionState.UNSUBSCRIBE -> SubscriptionState.UNSUBSCRIBE
                                    else -> SubscriptionState.EXIT
                                }
                            }
                        }
                        SubscriptionState.CONSUME_EVENTS -> {
                            try {
                                try {
                                    for (msg in connection.inboundChannelRegistry.get(channel)!!) {
                                        when (msg) {
                                            is WebSocketInboundMessage.Push -> {
                                                eventData = eventData.setPayload(json.decodeFromJsonElement(payloadType, msg.data))
                                                this@channelFlow.send(eventData)
                                            }
                                            is WebSocketInboundMessage.Error -> {
                                                throw msg.toException()
                                            }
                                            is WebSocketInboundMessage.Response -> {
                                                throw IllegalStateException("Subscribe/Unsubscribe event can't be received during events consumption")
                                            }
                                        }
                                    }
                                } catch (e: CancellationException) {
                                    throw e
                                } catch (e: DisconnectedException) {
                                    eventData = eventData.setError(e)
                                    state = SubscriptionState.EXIT
                                    this@channelFlow.send(eventData)
                                } catch (e: Throwable) {
                                    eventData = eventData.setError(e)
                                    state = SubscriptionState.UNSUBSCRIBE
                                    this@channelFlow.send(eventData)
                                }
                            } catch (e: CancellationException) {
                                state = when (state) {
                                    SubscriptionState.EXIT -> SubscriptionState.EXIT
                                    else -> SubscriptionState.UNSUBSCRIBE
                                }
                            }
                        }
                        SubscriptionState.UNSUBSCRIBE -> {
                            val request = WebSocketOutboundMessage(WebSocketOutboundMessage.Method.UNSUBSCRIBE, listOf(channel), connection.generateId())
                            val internalRequest = InternalWebSocketOutboundMessage(request, connection.inboundChannelRegistry.get(channel)!!)
                            state = try {
                                withContext(NonCancellable) {
                                    connection.outboundChannel.send(internalRequest)
                                }
                                SubscriptionState.CONFIRM_UNSUBSCRIPTION
                            } catch (e: CancellationException) {
                                SubscriptionState.CONFIRM_UNSUBSCRIPTION
                            } catch (e: DisconnectedException) {
                                SubscriptionState.EXIT
                            }
                        }
                        SubscriptionState.CONFIRM_UNSUBSCRIPTION -> {
                            try {
                                withContext(NonCancellable) {
                                    try {
                                        withTimeout(1.5.minutes) {
                                            for (msg in connection.inboundChannelRegistry.get(channel)!!) {
                                                when (msg) {
                                                    is WebSocketInboundMessage.Push -> {
                                                        eventData = eventData.setPayload(json.decodeFromJsonElement(payloadType, msg.data))
                                                        ignoreErrors { this@channelFlow.send(eventData) }
                                                    }
                                                    is WebSocketInboundMessage.Response -> {
                                                        eventData = eventData.setSubscribed(false)
                                                        state = SubscriptionState.EXIT
                                                        if (logger.isDebugEnabled) logger.debug("Unsubscribed from channel $channel")
                                                        this@channelFlow.send(eventData)
                                                        return@withTimeout
                                                    }
                                                    is WebSocketInboundMessage.Error -> {
                                                        throw msg.toException()
                                                    }
                                                }
                                            }
                                        }
                                    } catch (e: TimeoutCancellationException) {
                                        state = SubscriptionState.UNSUBSCRIBE
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: DisconnectedException) {
                                        eventData = eventData.setError(e)
                                        state = SubscriptionState.EXIT
                                        this@channelFlow.send(eventData)
                                    } catch (e: Throwable) {
                                        eventData = eventData.setError(e)
                                        state = SubscriptionState.UNSUBSCRIBE
                                        this@channelFlow.send(eventData)
                                    }
                                }
                            } catch (e: CancellationException) {
                                state = when (state) {
                                    SubscriptionState.UNSUBSCRIBE -> SubscriptionState.UNSUBSCRIBE
                                    else -> SubscriptionState.EXIT
                                }
                            }
                        }
                        SubscriptionState.EXIT -> {
                            connection.inboundChannelRegistry.remove(channel)?.close()
                            return@collect
                        }
                    }
                }
            }
        }

        fun subscribeToPrivateChannel() = channelFlow {
            suspend fun fetchListenKey(): Pair<String, Long> {
                val listenKey = try {
                    logger.info("Trying to fetch listen key...")
                    val key = binanceFuturesApi.getListenKey()
                    logger.info("Listen key fetched")
                    key
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    logger.warn("Can't get listen key: ${e.message}")
                    throw e
                }

                return Pair(listenKey, LISTEN_KEY_PING_INTERVAL.toLongMilliseconds())
            }

            suspend fun keepListenKeyAlive(listenKey: String) {
                while (isActive) {
                    try {
                        logger.info("Ping listen key")
                        binanceFuturesApi.pingListenKey(listenKey)
                        logger.info("Pong listen key")
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        logger.warn("Can't ping listen key: ${e.message}")
                        throw e
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

        //region Support Models
        private class ConnectionData(
            val outboundChannel: Channel<InternalWebSocketOutboundMessage> = Channel(Channel.RENDEZVOUS),
            val inboundChannelRegistry: ChannelRegistry = ChannelRegistry(),
        ) {
            private val reqIdCounter = AtomicLong(0)
            fun generateId(): Long = reqIdCounter.getAndIncrement()

            fun isClosed(): Boolean {
                return outboundChannel.isClosedForSend
            }

            suspend fun close(error: DisconnectedException) {
                outboundChannel.close(error)
                inboundChannelRegistry.close(error)
            }

            class ChannelRegistry {
                private val mutex = Mutex()
                private var closed = AtomicBoolean(false)
                private val registry = ConcurrentHashMap<String, Channel<WebSocketInboundMessage>>()

                suspend fun register(channelKey: String, channel: Channel<WebSocketInboundMessage>): Boolean {
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

                fun get(channelKey: String): Channel<WebSocketInboundMessage>? {
                    return registry[channelKey]
                }

                fun remove(channelKey: String): Channel<WebSocketInboundMessage>? {
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

        private enum class SubscriptionState {
            INIT,
            SUBSCRIBE,
            CONFIRM_SUBSCRIPTION,
            CONSUME_EVENTS,
            UNSUBSCRIBE,
            CONFIRM_UNSUBSCRIPTION,
            EXIT,
        }

        private data class InternalWebSocketOutboundMessage(
            val outboundMessage: WebSocketOutboundMessage,
            val inboundChannel: Channel<WebSocketInboundMessage>,
        )
        //endregion

        @Serializable
        private data class WebSocketOutboundMessage(
            val method: Method,
            val params: List<String>,
            val id: Long,
        ) {
            enum class Method {
                SUBSCRIBE,
                UNSUBSCRIBE,
                LIST_SUBSCRIPTIONS,
                GET_PROPERTY,
                SET_PROPERTY,
            }
        }

        @Serializable
        private sealed class WebSocketInboundMessage {
            @Serializable
            data class Push(
                val stream: String,
                val data: JsonElement,
            ) : WebSocketInboundMessage()

            @Serializable
            data class Response(
                val result: JsonElement,
                val id: Long,
            ) : WebSocketInboundMessage()

            @Serializable
            data class Error(
                val code: Long,
                val msg: String,
                val id: Long? = null,
            ) : WebSocketInboundMessage()
        }

        companion object {
            private val logger = KotlinLogging.logger {}

            private const val API_STREAM_URL_MAIN_NET = "wss://fstream.binance.com/stream"
            private const val API_STREAM_URL_TEST_NET = "wss://stream.binancefuture.com/stream"

            private val LISTEN_KEY_PING_INTERVAL = 45.minutes

            private fun WebSocketInboundMessage.Error.toException() = Exception(code, msg)
        }
    }

    private class HttpConnector(
        apiNet: ApiNet,
        val apiKey: String,
        apiSecret: String,
        val json: Json,
    ) {
        private val apiUrl = when (apiNet) {
            ApiNet.Main -> API_URL_MAIN_NET
            ApiNet.Test -> API_URL_TEST_NET
        }

        private val signer = HmacSha256Signer(apiSecret, toHexString)

        private val webClient: WebClient = springWebClient(
            connectTimeoutMs = 5000,
            readTimeoutMs = 5000,
            writeTimeoutMs = 5000,
            maxInMemorySize = 2 * 1024 * 1024,
        )

        suspend fun <T> callApi(
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
                request = request.header(API_KEY_HEADER, apiKey)
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

        @Serializable
        private data class ErrorMsg(
            val code: Long,
            val msg: String,
        )

        companion object {
            private const val API_URL_MAIN_NET = "https://fapi.binance.com"
            private const val API_URL_TEST_NET = "https://testnet.binancefuture.com"

            private const val API_KEY_HEADER = "X-MBX-APIKEY"
            private const val SIGNATURE = "signature"

            private fun Map<String, String>.toQueryString() = asSequence().map { "${it.key}=${it.value}" }.joinToString("&")
            private fun String.appendToQueryString(key: String, value: String) = "${if (isBlank()) "" else "$this&"}$key=$value"
            private fun String.appendToUri() = if (isBlank()) "" else "?$this"
        }
    }
}
