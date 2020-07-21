package com.gitlab.dhorman.cryptotrader.service.binance

import com.gitlab.dhorman.cryptotrader.util.readString
import com.gitlab.dhorman.cryptotrader.util.share
import com.gitlab.dhorman.cryptotrader.util.toHexString
import com.gitlab.dhorman.cryptotrader.util.writeString
import io.netty.channel.ChannelOption
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
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
import kotlinx.serialization.builtins.list
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.contentOrNull
import mu.KotlinLogging
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.netty.http.client.HttpClient
import reactor.netty.tcp.ProxyProvider
import java.io.File
import java.math.BigDecimal
import java.net.URI
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.ClosedChannelException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.security.PrivateKey
import java.security.Signature
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.collections.HashMap
import kotlin.time.hours
import kotlin.time.minutes
import kotlin.time.seconds
import kotlin.time.toJavaDuration

open class BinanceApi(
    private val apiKey: String? = null,
    apiSecret: String? = null,
    privateKey: PrivateKey? = null,
    apiNet: ApiNet,
    fileCachePath: String? = null
) {
    private val logger = KotlinLogging.logger {}
    private val signer: Signer?

    private val apiUrl: String
    private val apiUrlStream: String

    private val webClient: WebClient = createHttpClient()
    private val webSocketClient: WebSocketClient = createWebsocketClient()

    private val json: Json

    private val streamCache = ConcurrentHashMap<String, Flow<*>>()
    private val reqLimitsCache = ConcurrentHashMap<String, Long>()
    private val limitViolation = AtomicReference<LimitViolation?>(null)

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("BinanceApi"))
    private val closed = AtomicBoolean(false)

    private val exchangeInfoFileChannel: AsynchronousFileChannel?
    private val wsListenKeyFileChannel: AsynchronousFileChannel?
    private val feeInfoFileChannel: AsynchronousFileChannel?

    init {
        when (apiNet) {
            ApiNet.Main -> {
                apiUrl = "https://api.binance.com"
                apiUrlStream = "wss://stream.binance.com:9443/stream"

                signer = if (apiKey != null && apiSecret != null) {
                    HmacSha256Signer(apiSecret)
                } else {
                    null
                }
            }
            ApiNet.Test -> {
                apiUrl = "https://testnet.binance.vision"
                apiUrlStream = "wss://testnet.binance.vision/stream"

                signer = if (apiKey != null) {
                    when {
                        apiSecret != null -> HmacSha256Signer(apiSecret)
                        privateKey != null -> RsaSigner(privateKey)
                        else -> null
                    }
                } else {
                    null
                }
            }
        }

        json = Json {
            ignoreUnknownKeys = true
        }

        if (fileCachePath != null) {
            try {
                Files.createDirectories(Paths.get(fileCachePath))

                val exchangeInfoFilePath = Paths.get("$fileCachePath${File.separator}exchange_info.json")
                exchangeInfoFileChannel = AsynchronousFileChannel.open(exchangeInfoFilePath, *FILE_OPTIONS)

                val wsListenKeyFilePath = Paths.get("$fileCachePath${File.separator}ws_listen_key.json")
                wsListenKeyFileChannel = AsynchronousFileChannel.open(wsListenKeyFilePath, *FILE_OPTIONS)

                val feeInfoFilePath = Paths.get("$fileCachePath${File.separator}fee.json")
                feeInfoFileChannel = AsynchronousFileChannel.open(feeInfoFilePath, *FILE_OPTIONS)
            } catch (e: Throwable) {
                throw e
            }
        } else {
            exchangeInfoFileChannel = null
            wsListenKeyFileChannel = null
            feeInfoFileChannel = null
        }

        Runtime.getRuntime().addShutdownHook(Thread { runBlocking { close() } })
    }

    //region Maintenance
    suspend fun close() {
        if (closed.getAndSet(true)) return
        scope.coroutineContext[Job]?.cancelAndJoin()

        if (exchangeInfoFileChannel != null) ignoreErrors { exchangeInfoFileChannel.close() }
        if (wsListenKeyFileChannel != null) ignoreErrors { wsListenKeyFileChannel.close() }
    }
    //endregion

    //region Wallet API
    suspend fun systemStatus(): SystemStatus {
        return callApi("/wapi/v3/systemStatus.html", HttpMethod.GET, emptyMap(), false, false, type())
    }

    suspend fun getUserCoins(timestamp: Instant, recvWindow: Long? = null): List<UserCoin> {
        val params = HashMap<String, String>()
        params["timestamp"] = timestamp.toEpochMilli().toString()
        if (recvWindow != null) params["recvWindow"] = recvWindow.toString()
        return callApi("/sapi/v1/capital/config/getall", HttpMethod.GET, params, true, true, UserCoin.serializer().list)
    }

    suspend fun tradeFee(timestamp: Instant, recvWindow: Long? = null, symbol: String? = null): TradeFee {
        val params = buildMap<String, String> {
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
            if (symbol != null) put("symbol", symbol)
        }
        return callApi("/wapi/v3/tradeFee.html", HttpMethod.GET, params, true, true, type())
    }
    //endregion

    //region Wallet API Cached
    val tradeFeeCache: Flow<Map<String, CachedFee>> = run {
        val dataFetchInterval = 6.hours
        val flowCloseIntervalWhenNoSubscribers = dataFetchInterval.minus(1.hours)

        channelFlow {
            var cachedFeeInfo = if (feeInfoFileChannel != null) {
                try {
                    val feeInfoJsonString = feeInfoFileChannel.readString()
                    json.parse<CachedFeeInfo>(feeInfoJsonString)
                } catch (e: Throwable) {
                    null
                }
            } else {
                null
            }

            if (cachedFeeInfo != null) {
                send(cachedFeeInfo.fee)
                logger.info("Fee info extracted from cache")
                val expireTime = cachedFeeInfo.createTime.plusMillis(dataFetchInterval.toLongMilliseconds()).toEpochMilli()
                val now = Instant.now().toEpochMilli()
                val diffMs = expireTime - now
                if (diffMs > 0) delay(diffMs)
            }

            while (isActive) {
                try {
                    logger.info("Fetching fee info...")
                    val feeInfo = tradeFee(Instant.now())
                    logger.info("Fee info fetched successfully")

                    val symbolFeeMap = feeInfo.tradeFee.groupBy { it.symbol }.mapValues { it.value.first().toCachedFee() }
                    cachedFeeInfo = CachedFeeInfo(symbolFeeMap)
                    send(symbolFeeMap)

                    if (feeInfoFileChannel != null) {
                        ignoreErrors {
                            val feeInfoJsonString = json.stringify(cachedFeeInfo)
                            feeInfoFileChannel.writeString(feeInfoJsonString)
                        }
                    }

                    delay(dataFetchInterval)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    logger.warn("Can't fetch fee info: ${e.message}")
                    delay(1000)
                }
            }
        }
            .share(1, flowCloseIntervalWhenNoSubscribers.toJavaDuration(), scope)
    }
    //endregion

    //region Market Data API
    suspend fun ping() {
        return callApi("/api/v3/ping", HttpMethod.GET, emptyMap(), false, false, type())
    }

    suspend fun getExchangeInfo(): ExchangeInfo {
        return getExchangeInfo(false)
    }

    private suspend fun getExchangeInfo(initLimits: Boolean): ExchangeInfo {
        return callApi("/api/v3/exchangeInfo", HttpMethod.GET, emptyMap(), false, false, type(), initLimits)
    }

    suspend fun getCandlestickData(
        symbol: String,
        interval: CandleStickInterval,
        startTime: Instant,
        endTime: Instant,
        limit: Int = 500
    ): List<CandlestickData> {
        val params = HashMap<String, String>()
        params["symbol"] = symbol
        params["interval"] = interval.id
        params["startTime"] = startTime.toEpochMilli().toString()
        params["endTime"] = endTime.toEpochMilli().toString()
        params["limit"] = limit.toString()
        return callApi("/api/v3/klines", HttpMethod.GET, params, false, false, CandlestickData.CandlestickDataDeserializer.list)
    }

    suspend fun getOrderBook(symbol: String, limit: Int = 100): OrderBook {
        val params = mapOf(
            Pair("symbol", symbol),
            Pair("limit", limit.toString())
        )
        return callApi("/api/v3/depth", HttpMethod.GET, params, false, false, type())
    }
    //endregion

    //region Market Data Cached
    val exchangeInfoCache: Flow<ExchangeInfo> = run {
        val dataFetchInterval = 24.hours
        val flowCloseIntervalWhenNoSubscribers = dataFetchInterval.minus(1.hours)

        channelFlow {
            var cachedExchangeInfo = if (exchangeInfoFileChannel != null) {
                try {
                    val exchangeInfoJsonString = exchangeInfoFileChannel.readString()
                    json.parse<CachedExchangeInfo>(exchangeInfoJsonString)
                } catch (e: Throwable) {
                    null
                }
            } else {
                null
            }

            if (cachedExchangeInfo != null) {
                send(cachedExchangeInfo.exchangeInfo)
                logger.info("Exchange info extracted from cache")
                val expireTime = cachedExchangeInfo.createTime.plusMillis(dataFetchInterval.toLongMilliseconds()).toEpochMilli()
                val now = Instant.now().toEpochMilli()
                val diffMs = expireTime - now
                if (diffMs > 0) delay(diffMs)
            }

            while (isActive) {
                try {
                    logger.info("Fetching exchange info...")
                    val exchangeInfo = getExchangeInfo(cachedExchangeInfo == null)
                    logger.info("Exchange info fetched successfully")

                    cachedExchangeInfo = CachedExchangeInfo(exchangeInfo)
                    send(exchangeInfo)

                    if (exchangeInfoFileChannel != null) {
                        ignoreErrors {
                            val exchangeInfoJsonString = json.stringify(cachedExchangeInfo)
                            exchangeInfoFileChannel.writeString(exchangeInfoJsonString)
                        }
                    }

                    delay(dataFetchInterval)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    logger.warn("Can't fetch exchange info: ${e.message}")
                    delay(1000)
                }
            }
        }
            .share(1, flowCloseIntervalWhenNoSubscribers.toJavaDuration(), scope)
    }
    //endregion

    //region Spot Account/Trade API
    suspend fun testNewOrder(
        symbol: String,
        side: OrderSide,
        type: OrderType,
        timestamp: Instant,
        timeInForce: TimeInForce? = null,
        quantity: BigDecimal? = null,
        quoteOrderQty: BigDecimal? = null,
        price: BigDecimal? = null,
        newClientOrderId: String? = null,
        stopPrice: BigDecimal? = null,
        icebergQty: BigDecimal? = null,
        newOrderRespType: OrderRespType? = null,
        recvWindow: Long? = null
    ): NewOrder {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("side", side.toString())
            put("type", type.toString())
            put("timestamp", timestamp.toEpochMilli().toString())
            if (timeInForce != null) put("timeInForce", timeInForce.toString())
            if (quantity != null) put("quantity", quantity.toPlainString())
            if (quoteOrderQty != null) put("quoteOrderQty", quoteOrderQty.toPlainString())
            if (price != null) put("price", price.toPlainString())
            if (newClientOrderId != null) put("newClientOrderId", newClientOrderId)
            if (stopPrice != null) put("stopPrice", stopPrice.toPlainString())
            if (icebergQty != null) put("icebergQty", icebergQty.toPlainString())
            if (newOrderRespType != null) put("newOrderRespType", newOrderRespType.toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return callApi("/api/v3/order/test", HttpMethod.POST, params, true, true, type())
    }

    suspend fun placeOrder(
        symbol: String,
        side: OrderSide,
        type: OrderType,
        timestamp: Instant,
        timeInForce: TimeInForce? = null,
        quantity: BigDecimal? = null,
        quoteOrderQty: BigDecimal? = null,
        price: BigDecimal? = null,
        newClientOrderId: String? = null,
        stopPrice: BigDecimal? = null,
        icebergQty: BigDecimal? = null,
        newOrderRespType: OrderRespType? = null,
        recvWindow: Long? = null
    ): NewOrder {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("side", side.toString())
            put("type", type.toString())
            put("timestamp", timestamp.toEpochMilli().toString())
            if (timeInForce != null) put("timeInForce", timeInForce.toString())
            if (quantity != null) put("quantity", quantity.toPlainString())
            if (quoteOrderQty != null) put("quoteOrderQty", quoteOrderQty.toPlainString())
            if (price != null) put("price", price.toPlainString())
            if (newClientOrderId != null) put("newClientOrderId", newClientOrderId)
            if (stopPrice != null) put("stopPrice", stopPrice.toPlainString())
            if (icebergQty != null) put("icebergQty", icebergQty.toPlainString())
            if (newOrderRespType != null) put("newOrderRespType", newOrderRespType.toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return callApi("/api/v3/order", HttpMethod.POST, params, true, true, type())
    }

    suspend fun cancelOrder(
        symbol: String,
        timestamp: Instant,
        orderId: Long? = null,
        origClientOrderId: String? = null,
        newClientOrderId: String? = null,
        recvWindow: Long? = null
    ): CancelOrder {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("timestamp", timestamp.toEpochMilli().toString())

            when {
                orderId != null -> put("orderId", orderId.toString())
                origClientOrderId != null -> put("origClientOrderId", origClientOrderId)
                else -> throw Error.MANDATORY_PARAM_EMPTY_OR_MALFORMED.toException()
            }

            if (newClientOrderId != null) put("newClientOrderId", newClientOrderId)
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return callApi("/api/v3/order", HttpMethod.DELETE, params, true, true, type())
    }

    suspend fun cancelAllOrders(symbol: String, timestamp: Instant, recvWindow: Long? = null): List<CanceledOrder> {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("timestamp", timestamp.toEpochMilli().toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return callApi("/api/v3/openOrders", HttpMethod.DELETE, params, true, true, CanceledOrder.serializer().list)
    }

    suspend fun getAccountInfo(timestamp: Instant, recvWindow: Long? = null): AccountInfo {
        val params = HashMap<String, String>()
        params["timestamp"] = timestamp.toEpochMilli().toString()
        if (recvWindow != null) params["recvWindow"] = recvWindow.toString()
        return callApi("/api/v3/account", HttpMethod.GET, params, true, true, type())
    }

    suspend fun getAccountTrades(
        symbol: String,
        timestamp: Instant,
        startTime: Instant? = null,
        endTime: Instant? = null,
        fromId: Long? = null,
        limit: Int? = null,
        recvWindow: Long? = null
    ): List<AccountTrade> {
        val params = buildMap<String, String> {
            put("symbol", symbol)
            put("timestamp", timestamp.toEpochMilli().toString())
            if (startTime != null) put("startTime", startTime.toEpochMilli().toString())
            if (endTime != null) put("endTime", endTime.toEpochMilli().toString())
            if (fromId != null) put("fromId", fromId.toString())
            if (limit != null) put("limit", limit.toString())
            if (recvWindow != null) put("recvWindow", recvWindow.toString())
        }
        return callApi("/api/v3/myTrades", HttpMethod.GET, params, true, true, AccountTrade.serializer().list)
    }
    //endregion

    //region Market Streams API
    fun aggregateTradeStream(symbol: String): Flow<EventData<AggregateTradeEvent>> {
        return subscribeToCached("$symbol@aggTrade") { subscribeTo(it, type<AggregateTradeEvent>()) }
    }

    fun tradeStream(symbol: String): Flow<EventData<TradeEvent>> {
        return subscribeToCached("$symbol@trade") { subscribeTo(it, type<TradeEvent>()) }
    }

    fun candlestickStream(symbol: String, interval: CandleStickInterval): Flow<EventData<CandlestickEvent>> {
        return subscribeToCached("$symbol@kline_${interval.id}") { subscribeTo(it, type<CandlestickEvent>()) }
    }

    fun individualSymbolMiniTickerStream(symbol: String): Flow<EventData<MiniTickerEvent>> {
        return subscribeToCached("$symbol@miniTicker") { subscribeTo(it, type<MiniTickerEvent>()) }
    }

    val allMarketMiniTickersStream: Flow<EventData<List<MiniTickerEvent>>> = run {
        subscribeTo("!miniTicker@arr", MiniTickerEvent.serializer().list).share()
    }

    fun individualSymbolTickerStream(symbol: String): Flow<EventData<TickerEvent>> {
        return subscribeToCached("$symbol@ticker") { subscribeTo(it, type<TickerEvent>()) }
    }

    val allMarketTickersStream: Flow<EventData<List<TickerEvent>>> = run {
        subscribeTo("!ticker@arr", TickerEvent.serializer().list).share()
    }

    fun individualSymbolBookTickerStream(symbol: String): Flow<EventData<BookTickerEvent>> {
        return subscribeToCached("$symbol@bookTicker") { subscribeTo(it, type<BookTickerEvent>()) }
    }

    val allBookTickerStream: Flow<EventData<BookTickerEvent>> = run {
        subscribeTo("!bookTicker", type<BookTickerEvent>()).share()
    }

    fun partialBookDepthStream(
        symbol: String,
        level: PartialBookDepthEvent.Level,
        updateSpeed: BookUpdateSpeed? = null
    ): Flow<EventData<PartialBookDepthEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeMs}ms"
        return subscribeToCached("$symbol@depth${level.id}$updateSpeedStr") { subscribeTo(it, type<PartialBookDepthEvent>()) }
    }

    fun diffDepthStream(
        symbol: String,
        updateSpeed: BookUpdateSpeed? = null
    ): Flow<EventData<DiffDepthEvent>> {
        val updateSpeedStr = if (updateSpeed == null) "" else "@${updateSpeed.timeMs}ms"
        return subscribeToCached("$symbol@depth$updateSpeedStr") { subscribeTo(it, type<DiffDepthEvent>()) }
    }
    //endregion

    //region User Data Streams Public API
    val accountStream: Flow<EventData<AccountEvent>> = run {
        subscribeToPrivateChannel().share()
    }

    val accountUpdatesStream: Flow<EventData<AccountUpdateEvent>> = run {
        accountStream.filterAccountEvent()
    }

    val balanceUpdatesStream: Flow<EventData<BalanceUpdateEvent>> = run {
        accountStream.filterAccountEvent()
    }

    val orderUpdatesStream: Flow<EventData<OrderUpdateEvent>> = run {
        accountStream.filterAccountEvent()
    }
    //endregion

    //region User Data Streams Private API
    private suspend fun getListenKey(): String {
        val resp = callApi("/api/v3/userDataStream", HttpMethod.POST, emptyMap(), true, false, type<ListenKey>())
        return resp.listenKey
    }

    private suspend fun pingListenKey(listenKey: String) {
        val params = mapOf(Pair("listenKey", listenKey))
        callApi("/api/v3/userDataStream", HttpMethod.PUT, params, true, false, type<Unit>())
    }

    private suspend fun deleteListenKey(listenKey: String) {
        val params = mapOf(Pair("listenKey", listenKey))
        callApi("/api/v3/userDataStream", HttpMethod.DELETE, params, true, false, type<Unit>())
    }
    //endregion

    //region Public Models
    enum class ApiNet {
        Main,
        Test
    }

    @Serializable(CandleStickInterval.Companion.CandleStickIntervalSerializer::class)
    enum class CandleStickInterval(val id: String) {
        INTERVAL_1_MINUTE("1m"),
        INTERVAL_3_MINUTES("3m"),
        INTERVAL_5_MINUTES("5m"),
        INTERVAL_15_MINUTES("15m"),
        INTERVAL_30_MINUTES("30m"),
        INTERVAL_1_HOUR("1h"),
        INTERVAL_2_HOURS("2h"),
        INTERVAL_4_HOURS("4h"),
        INTERVAL_6_HOURS("6h"),
        INTERVAL_8_HOURS("8h"),
        INTERVAL_12_HOURS("12h"),
        INTERVAL_1_DAY("1d"),
        INTERVAL_3_DAYS("3d"),
        INTERVAL_1_WEEK("1w"),
        INTERVAL_1_MONTH("1M");

        companion object {
            object CandleStickIntervalSerializer : KSerializer<CandleStickInterval> {
                override val descriptor: SerialDescriptor = PrimitiveDescriptor("BinanceCandleStickIntervalSerializer", PrimitiveKind.STRING)

                override fun deserialize(decoder: Decoder): CandleStickInterval {
                    return when (val intervalStr = decoder.decodeString()) {
                        "1m" -> INTERVAL_1_MINUTE
                        "3m" -> INTERVAL_3_MINUTES
                        "5m" -> INTERVAL_5_MINUTES
                        "15m" -> INTERVAL_15_MINUTES
                        "30m" -> INTERVAL_30_MINUTES
                        "1h" -> INTERVAL_1_HOUR
                        "2h" -> INTERVAL_2_HOURS
                        "4h" -> INTERVAL_4_HOURS
                        "6h" -> INTERVAL_6_HOURS
                        "8h" -> INTERVAL_8_HOURS
                        "12h" -> INTERVAL_12_HOURS
                        "1d" -> INTERVAL_1_DAY
                        "3d" -> INTERVAL_3_DAYS
                        "1w" -> INTERVAL_1_WEEK
                        "1M" -> INTERVAL_1_MONTH
                        else -> throw SerializationException("Not recognized candlestick interval $intervalStr")
                    }
                }

                override fun serialize(encoder: Encoder, value: CandleStickInterval) {
                    encoder.encodeString(value.id)
                }
            }
        }
    }

    enum class OrderType {
        LIMIT,
        MARKET,
        STOP_LOSS,
        STOP_LOSS_LIMIT,
        TAKE_PROFIT,
        TAKE_PROFIT_LIMIT,
        LIMIT_MAKER,
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
        PENDING_CANCEL,
        REJECTED,
        EXPIRED,
    }

    enum class TimeInForce {
        GTC,
        IOC,
        FOK,
    }

    enum class OrderRespType {
        ACK,
        RESULT,
        FULL,
    }

    enum class RateLimitType {
        REQUEST_WEIGHT,
        ORDERS,
        RAW_REQUESTS,
    }

    enum class RateLimitInterval {
        SECOND,
        MINUTE,
        HOUR,
        DAY,
    }

    enum class TradingPermission {
        SPOT,
        MARGIN,
        LEVERAGED,
    }

    enum class ContingencyType {
        OCO
    }

    enum class OCOStatus {
        RESPONSE,
        EXEC_STARTED,
        ALL_DONE,
    }

    enum class OCOOrderStatus {
        EXECUTING,
        ALL_DONE,
        REJECT,
    }

    @Serializable
    data class SystemStatus(
        val status: Int,
        val msg: String
    )

    @Serializable
    data class UserCoin(
        val coin: String,
        val depositAllEnable: Boolean,
        val withdrawAllEnable: Boolean,
        val name: String,
        @Serializable(BigDecimalStringSerializer::class) val free: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val locked: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val freeze: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val withdrawing: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val ipoing: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val ipoable: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val storage: BigDecimal,
        val isLegalMoney: Boolean,
        val trading: Boolean,
        val networkList: List<Network>
    ) {
        @Serializable
        data class Network(
            val network: String,
            val coin: String,
            @Serializable(BigDecimalStringSerializer::class) val withdrawIntegerMultiple: BigDecimal? = null,
            val isDefault: Boolean,
            val depositEnable: Boolean,
            val withdrawEnable: Boolean,
            val depositDesc: String? = null,
            val withdrawDesc: String? = null,
            val specialTips: String? = null,
            val name: String,
            val resetAddressStatus: Boolean,
            val addressRegex: String,
            val memoRegex: String,
            @Serializable(BigDecimalStringSerializer::class) val withdrawFee: BigDecimal,
            @Serializable(BigDecimalStringSerializer::class) val withdrawMin: BigDecimal,
            @Serializable(BigDecimalStringSerializer::class) val withdrawMax: BigDecimal? = null,
            val minConfirm: Long? = null,
            val unLockConfirm: Long? = null
        )
    }

    @Serializable
    data class TradeFee(
        val tradeFee: List<Fee>,
        val success: Boolean
    ) {
        @Serializable
        data class Fee(
            val symbol: String,
            @Serializable(BigDecimalDoubleSerializer::class) val maker: BigDecimal,
            @Serializable(BigDecimalDoubleSerializer::class) val taker: BigDecimal
        )
    }

    @Serializable
    data class CachedFee(
        @Serializable(BigDecimalStringSerializer::class) val maker: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val taker: BigDecimal
    )

    @Serializable
    data class ExchangeInfo(
        val timezone: String,
        @Serializable(InstantLongSerializer::class) val serverTime: Instant,
        val rateLimits: List<RateLimit>,
        val exchangeFilters: List<ExchangeFilter>,
        val symbols: List<Symbol>
    ) {
        enum class ExchangeFilter {
            EXCHANGE_MAX_NUM_ORDERS,
            EXCHANGE_MAX_NUM_ALGO_ORDERS,
        }

        @Serializable
        data class RateLimit(
            val rateLimitType: RateLimitType,
            val interval: RateLimitInterval,
            val intervalNum: Long,
            val limit: Long
        ) {
            @Transient
            val httpHeader = "${rateLimitType.headerPrefix()}-$intervalNum${interval.toShortDescription()}"

            private fun RateLimitType.headerPrefix() = when (this) {
                RateLimitType.REQUEST_WEIGHT -> "X-MBX-USED-WEIGHT"
                RateLimitType.ORDERS -> "X-MBX-ORDER-COUNT"
                RateLimitType.RAW_REQUESTS -> "X-MBX-REQ-COUNT" // TODO: Check this header name
            }

            private fun RateLimitInterval.toShortDescription() = when (this) {
                RateLimitInterval.SECOND -> "s"
                RateLimitInterval.MINUTE -> "m"
                RateLimitInterval.HOUR -> "h"
                RateLimitInterval.DAY -> "d"
            }
        }

        @Serializable
        data class Symbol(
            val symbol: String,
            val status: Status,
            val baseAsset: String,
            val baseAssetPrecision: Int,
            val quoteAsset: String,
            val quotePrecision: Int,
            val quoteAssetPrecision: Int,
            val baseCommissionPrecision: Int,
            val quoteCommissionPrecision: Int,
            val orderTypes: List<OrderType>,
            val icebergAllowed: Boolean,
            val ocoAllowed: Boolean,
            val quoteOrderQtyMarketAllowed: Boolean,
            val isSpotTradingAllowed: Boolean,
            val isMarginTradingAllowed: Boolean,
            val filters: List<Filter>,
            val permissions: List<TradingPermission>
        ) {
            @Serializable
            enum class Status {
                PRE_TRADING,
                TRADING,
                POST_TRADING,
                END_OF_DAY,
                HALT,
                AUCTION_MATCH,
                BREAK,
            }

            @Serializable
            data class Filter(
                val filterType: Type,
                @Serializable(BigDecimalStringSerializer::class) val minPrice: BigDecimal? = null,
                @Serializable(BigDecimalStringSerializer::class) val maxPrice: BigDecimal? = null,
                @Serializable(BigDecimalStringSerializer::class) val tickSize: BigDecimal? = null,
                @Serializable(BigDecimalStringSerializer::class) val multiplierUp: BigDecimal? = null,
                @Serializable(BigDecimalStringSerializer::class) val multiplierDown: BigDecimal? = null,
                val avgPriceMins: Long? = null,
                @Serializable(BigDecimalStringSerializer::class) val minQty: BigDecimal? = null,
                @Serializable(BigDecimalStringSerializer::class) val maxQty: BigDecimal? = null,
                @Serializable(BigDecimalStringSerializer::class) val stepSize: BigDecimal? = null,
                @Serializable(BigDecimalStringSerializer::class) val minNotional: BigDecimal? = null,
                val applyToMarket: Boolean? = null,
                val limit: Long? = null,
                val maxNumOrders: Long? = null,
                val maxNumAlgoOrders: Long? = null
            ) {
                enum class Type {
                    PRICE_FILTER,
                    PERCENT_PRICE,
                    LOT_SIZE,
                    MIN_NOTIONAL,
                    ICEBERG_PARTS,
                    MARKET_LOT_SIZE,
                    MAX_NUM_ORDERS,
                    MAX_NUM_ALGO_ORDERS,
                    MAX_NUM_ICEBERG_ORDERS,
                    MAX_POSITION,
                }
            }
        }
    }

    data class CandlestickData(
        val openTime: Instant,
        val open: BigDecimal,
        val high: BigDecimal,
        val low: BigDecimal,
        val close: BigDecimal,
        val volume: BigDecimal,
        val closeTime: Instant,
        val quoteAssetVolume: BigDecimal,
        val tradesCount: Long,
        val takerBuyBaseAssetVolume: BigDecimal,
        val takerBuyQuoteAssetVolume: BigDecimal,
        val ignore: BigDecimal
    ) {
        object CandlestickDataDeserializer : KSerializer<CandlestickData> {
            override val descriptor: SerialDescriptor = SerialDescriptor("BinanceCandlestickDataDeserializer", StructureKind.LIST)

            override fun deserialize(decoder: Decoder): CandlestickData {
                return decoder.decodeStructure(descriptor) {
                    var openTime: Instant? = null
                    var open: BigDecimal? = null
                    var high: BigDecimal? = null
                    var low: BigDecimal? = null
                    var close: BigDecimal? = null
                    var volume: BigDecimal? = null
                    var closeTime: Instant? = null
                    var quoteAssetVolume: BigDecimal? = null
                    var tradesCount: Long? = null
                    var takerBuyBaseAssetVolume: BigDecimal? = null
                    var takerBuyQuoteAssetVolume: BigDecimal? = null
                    var ignore: BigDecimal? = null

                    loop@ while (true) {
                        when (val index = decodeElementIndex(descriptor)) {
                            CompositeDecoder.READ_DONE -> break@loop
                            0 -> openTime = decodeSerializableElement(descriptor, index, InstantLongSerializer)
                            1 -> open = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            2 -> high = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            3 -> low = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            4 -> close = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            5 -> volume = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            6 -> closeTime = decodeSerializableElement(descriptor, index, InstantLongSerializer)
                            7 -> quoteAssetVolume = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            8 -> tradesCount = decodeLongElement(descriptor, index)
                            9 -> takerBuyBaseAssetVolume = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            10 -> takerBuyQuoteAssetVolume = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            11 -> ignore = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                            else -> {
                            }
                        }
                    }

                    CandlestickData(
                        openTime!!,
                        open!!,
                        high!!,
                        low!!,
                        close!!,
                        volume!!,
                        closeTime!!,
                        quoteAssetVolume!!,
                        tradesCount!!,
                        takerBuyBaseAssetVolume!!,
                        takerBuyQuoteAssetVolume!!,
                        ignore!!
                    )
                }
            }

            override fun serialize(encoder: Encoder, value: CandlestickData) = throw RuntimeException("Not implemented")
        }
    }

    @Serializable
    data class OrderBook(
        val lastUpdateId: Long,
        val bids: List<@Serializable(Record.RecordDeserializer::class) Record>,
        val asks: List<@Serializable(Record.RecordDeserializer::class) Record>
    ) {
        data class Record(
            val price: BigDecimal,
            val qty: BigDecimal
        ) {
            object RecordDeserializer : KSerializer<Record> {
                override val descriptor: SerialDescriptor = SerialDescriptor("BinanceOrderBookRecordDeserializer", StructureKind.LIST) {
                    element("price", BigDecimalStringSerializer.descriptor)
                    element("qty", BigDecimalStringSerializer.descriptor)
                }

                override fun deserialize(decoder: Decoder): Record {
                    return decoder.decodeStructure(descriptor) {
                        var price: BigDecimal? = null
                        var qty: BigDecimal? = null

                        loop@ while (true) {
                            when (val index = decodeElementIndex(descriptor)) {
                                CompositeDecoder.READ_DONE -> break@loop
                                0 -> price = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
                                1 -> qty = decodeSerializableElement(descriptor, index, BigDecimalStringSerializer)
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
    data class NewOrder(
        val symbol: String,
        val orderId: Long,
        val orderListId: Long,
        val clientOrderId: String,
        @Serializable(InstantLongSerializer::class) val transactTime: Instant,
        @Serializable(BigDecimalStringSerializer::class) val price: BigDecimal? = null,
        @Serializable(BigDecimalStringSerializer::class) val origQty: BigDecimal? = null,
        @Serializable(BigDecimalStringSerializer::class) val executedQty: BigDecimal? = null,
        @Serializable(BigDecimalStringSerializer::class) val cummulativeQuoteQty: BigDecimal? = null,
        val status: OrderStatus? = null,
        val timeInForce: TimeInForce? = null,
        val type: OrderType? = null,
        val side: OrderSide? = null,
        val fills: List<Order>? = null
    ) {
        @Serializable
        data class Order(
            @Serializable(BigDecimalStringSerializer::class) val price: BigDecimal,
            @Serializable(BigDecimalStringSerializer::class) val qty: BigDecimal,
            @Serializable(BigDecimalStringSerializer::class) val commission: BigDecimal,
            val commissionAsset: String
        )
    }

    @Serializable
    data class CancelOrder(
        val symbol: String,
        val origClientOrderId: String,
        val orderId: Long,
        val orderListId: Long,
        val clientOrderId: String,
        @Serializable(BigDecimalStringSerializer::class) val price: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val origQty: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val executedQty: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val cummulativeQuoteQty: BigDecimal,
        val status: OrderStatus,
        val timeInForce: TimeInForce,
        val type: OrderType,
        val side: OrderSide
    )

    @Serializable
    data class CanceledOrder(
        val symbol: String,
        val origClientOrderId: String,
        val orderId: Long,
        val orderListId: Long,
        val clientOrderId: String,
        @Serializable(BigDecimalStringSerializer::class) val price: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val origQty: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val executedQty: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val cummulativeQuoteQty: BigDecimal,
        val status: OrderStatus,
        val timeInForce: TimeInForce,
        val type: OrderType,
        val side: OrderSide
    )

    @Serializable
    data class AccountInfo(
        val makerCommission: Long,
        val takerCommission: Long,
        val buyerCommission: Long,
        val sellerCommission: Long,
        val canTrade: Boolean,
        val canWithdraw: Boolean,
        val canDeposit: Boolean,
        @Serializable(InstantLongSerializer::class) val updateTime: Instant,
        val accountType: String,
        val balances: List<Balance>,
        val permissions: List<String>
    ) {
        @Serializable
        data class Balance(
            val asset: String,
            @Serializable(BigDecimalStringSerializer::class) val free: BigDecimal,
            @Serializable(BigDecimalStringSerializer::class) val locked: BigDecimal
        )
    }

    @Serializable
    data class AccountTrade(
        val symbol: String,
        val id: Long,
        val orderId: Long,
        val orderListId: Long,
        @Serializable(BigDecimalStringSerializer::class) val price: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val qty: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val quoteQty: BigDecimal,
        @Serializable(BigDecimalStringSerializer::class) val commission: BigDecimal,
        val commissionAsset: String,
        @Serializable(InstantLongSerializer::class) val time: Instant,
        val isBuyer: Boolean,
        val isMaker: Boolean,
        val isBestMatch: Boolean
    )
    //endregion

    //region Public Events
    data class EventData<T>(
        val payload: T? = null,
        val subscribed: Boolean = false,
        val error: Throwable? = null
    )

    @Serializable
    data class AggregateTradeEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("a") val aggregateTradeId: Long,
        @SerialName("p") @Serializable(BigDecimalStringSerializer::class) val price: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalStringSerializer::class) val quantity: BigDecimal,
        @SerialName("f") val firstTradeId: Long,
        @SerialName("l") val lastTradeId: Long,
        @SerialName("T") @Serializable(InstantLongSerializer::class) val tradeTime: Instant,
        @SerialName("m") val buyerMarketMaker: Boolean,
        @SerialName("M") val ignore: Boolean
    )

    @Serializable
    data class TradeEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("t") val tradeId: Long,
        @SerialName("p") @Serializable(BigDecimalStringSerializer::class) val price: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalStringSerializer::class) val quantity: BigDecimal,
        @SerialName("b") val buyerOrderId: Long,
        @SerialName("a") val sellerOrderId: Long,
        @SerialName("T") @Serializable(InstantLongSerializer::class) val tradeTime: Instant,
        @SerialName("m") val buyerMarketMaker: Boolean,
        @SerialName("M") val ignore: Boolean
    )

    @Serializable
    data class CandlestickEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("k") val data: Data
    ) {
        @Serializable
        data class Data(
            @SerialName("t") @Serializable(InstantLongSerializer::class) val klineStartTime: Instant,
            @SerialName("T") @Serializable(InstantLongSerializer::class) val klineCloseTime: Instant,
            @SerialName("s") val symbol: String,
            @SerialName("i") val interval: CandleStickInterval,
            @SerialName("f") val firstTradeId: Long,
            @SerialName("L") val lastTradeId: Long,
            @SerialName("o") @Serializable(BigDecimalStringSerializer::class) val openPrice: BigDecimal,
            @SerialName("c") @Serializable(BigDecimalStringSerializer::class) val closePrice: BigDecimal,
            @SerialName("h") @Serializable(BigDecimalStringSerializer::class) val highPrice: BigDecimal,
            @SerialName("l") @Serializable(BigDecimalStringSerializer::class) val lowPrice: BigDecimal,
            @SerialName("v") @Serializable(BigDecimalStringSerializer::class) val baseAssetVolume: BigDecimal,
            @SerialName("n") val tradesCount: Long,
            @SerialName("x") val klineClosed: Boolean,
            @SerialName("q") @Serializable(BigDecimalStringSerializer::class) val quoteAssetVolume: BigDecimal,
            @SerialName("V") @Serializable(BigDecimalStringSerializer::class) val takerBuyBaseAssetVolume: BigDecimal,
            @SerialName("Q") @Serializable(BigDecimalStringSerializer::class) val takerBuyQuoteAssetVolume: BigDecimal,
            @SerialName("B") @Serializable(BigDecimalStringSerializer::class) val ignore: BigDecimal
        )
    }

    @Serializable
    data class MiniTickerEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("c") @Serializable(BigDecimalStringSerializer::class) val closePrice: BigDecimal,
        @SerialName("o") @Serializable(BigDecimalStringSerializer::class) val openPrice: BigDecimal,
        @SerialName("h") @Serializable(BigDecimalStringSerializer::class) val highPrice: BigDecimal,
        @SerialName("l") @Serializable(BigDecimalStringSerializer::class) val lowPrice: BigDecimal,
        @SerialName("v") @Serializable(BigDecimalStringSerializer::class) val totalTradedBaseAssetVolume: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalStringSerializer::class) val totalTradedQuoteAssetVolume: BigDecimal
    )

    @Serializable
    data class TickerEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("p") @Serializable(BigDecimalStringSerializer::class) val priceChange: BigDecimal,
        @SerialName("P") @Serializable(BigDecimalStringSerializer::class) val priceChangePercent: BigDecimal,
        @SerialName("w") @Serializable(BigDecimalStringSerializer::class) val weightedAveragePrice: BigDecimal,
        @SerialName("x") @Serializable(BigDecimalStringSerializer::class) val firstTradeBefore24hrRollingWindow: BigDecimal,
        @SerialName("c") @Serializable(BigDecimalStringSerializer::class) val lastPrice: BigDecimal,
        @SerialName("Q") @Serializable(BigDecimalStringSerializer::class) val lastQuantity: BigDecimal,
        @SerialName("b") @Serializable(BigDecimalStringSerializer::class) val bestBidPrice: BigDecimal,
        @SerialName("B") @Serializable(BigDecimalStringSerializer::class) val bestBidQuantity: BigDecimal,
        @SerialName("a") @Serializable(BigDecimalStringSerializer::class) val bestAskPrice: BigDecimal,
        @SerialName("A") @Serializable(BigDecimalStringSerializer::class) val bestAskQuantity: BigDecimal,
        @SerialName("o") @Serializable(BigDecimalStringSerializer::class) val openPrice: BigDecimal,
        @SerialName("h") @Serializable(BigDecimalStringSerializer::class) val highPrice: BigDecimal,
        @SerialName("l") @Serializable(BigDecimalStringSerializer::class) val lowPrice: BigDecimal,
        @SerialName("v") @Serializable(BigDecimalStringSerializer::class) val totalTradedBaseAssetVolume: BigDecimal,
        @SerialName("q") @Serializable(BigDecimalStringSerializer::class) val totalTradedQuoteAssetVolume: BigDecimal,
        @SerialName("O") @Serializable(InstantLongSerializer::class) val statisticsOpenTime: Instant,
        @SerialName("C") @Serializable(InstantLongSerializer::class) val statisticsCloseTime: Instant,
        @SerialName("F") val firstTradeId: Long,
        @SerialName("L") val lastTradeId: Long,
        @SerialName("n") val tradesCount: Long
    )

    @Serializable
    data class BookTickerEvent(
        @SerialName("u") val orderBookUpdateId: Long,
        @SerialName("s") val symbol: String,
        @SerialName("b") @Serializable(BigDecimalStringSerializer::class) val bestBidPrice: BigDecimal,
        @SerialName("B") @Serializable(BigDecimalStringSerializer::class) val bestBidQty: BigDecimal,
        @SerialName("a") @Serializable(BigDecimalStringSerializer::class) val bestAskPrice: BigDecimal,
        @SerialName("A") @Serializable(BigDecimalStringSerializer::class) val bestAskQty: BigDecimal
    )

    enum class BookUpdateSpeed(val timeMs: Long) {
        TIME_100_MS(100),
        TIME_1000_MS(1000),
    }

    @Serializable
    data class PartialBookDepthEvent(
        val lastUpdateId: Long,
        val bids: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>,
        val asks: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>
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
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("U") val firstUpdateIdInEvent: Long,
        @SerialName("b") val bids: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>,
        @SerialName("a") val asks: List<@Serializable(OrderBook.Record.RecordDeserializer::class) OrderBook.Record>
    )

    interface AccountEvent

    @Serializable
    data class AccountUpdateEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("m") val makerCommissionRate: Long,
        @SerialName("t") val takerCommissionRate: Long,
        @SerialName("b") val buyerCommissionRate: Long,
        @SerialName("s") val sellerCommissionRate: Long,
        @SerialName("T") val canTrade: Boolean,
        @SerialName("W") val canWithdraw: Boolean,
        @SerialName("D") val canDeposit: Boolean,
        @SerialName("u") @Serializable(InstantLongSerializer::class) val lastAccountUpdateTime: Instant,
        @SerialName("B") val balances: List<Balance>
    ) : AccountEvent {
        @Serializable
        data class Balance(
            @SerialName("a") val asset: String,
            @SerialName("f") @Serializable(BigDecimalStringSerializer::class) val freeAmount: BigDecimal,
            @SerialName("l") @Serializable(BigDecimalStringSerializer::class) val lockedAmount: BigDecimal
        )
    }

    @Serializable
    data class OutboundAccountPositionEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("u") @Serializable(InstantLongSerializer::class) val lastAccountUpdateTime: Instant,
        @SerialName("B") val balances: List<Balance>
    ) : AccountEvent {
        @Serializable
        data class Balance(
            @SerialName("a") val asset: String,
            @SerialName("f") @Serializable(BigDecimalStringSerializer::class) val free: BigDecimal,
            @SerialName("l") @Serializable(BigDecimalStringSerializer::class) val locked: BigDecimal
        )
    }

    @Serializable
    data class BalanceUpdateEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("a") val asset: String,
        @SerialName("d") @Serializable(BigDecimalStringSerializer::class) val balanceDelta: BigDecimal,
        @SerialName("T") @Serializable(InstantLongSerializer::class) val clearTime: Instant
    ) : AccountEvent

    @Serializable
    data class OrderUpdateEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("c") val clientOrderId: String,
        @SerialName("S") val side: OrderSide,
        @SerialName("o") val orderType: OrderType,
        @SerialName("f") val timeInForce: TimeInForce,
        @SerialName("q") @Serializable(BigDecimalStringSerializer::class) val orderQuantity: BigDecimal,
        @SerialName("p") @Serializable(BigDecimalStringSerializer::class) val orderPrice: BigDecimal,
        @SerialName("P") @Serializable(BigDecimalStringSerializer::class) val stopPrice: BigDecimal,
        @SerialName("F") @Serializable(BigDecimalStringSerializer::class) val icebergQuantity: BigDecimal,
        @SerialName("g") val orderListId: Long,
        @SerialName("C") val originalClientOrderId: String,
        @SerialName("x") val currentExecutionType: OrderStatus,
        @SerialName("X") val currentOrderStatus: OrderStatus,
        @SerialName("r") val orderRejectReason: String,
        @SerialName("i") val orderId: Long,
        @SerialName("l") @Serializable(BigDecimalStringSerializer::class) val lastExecutedQuantity: BigDecimal,
        @SerialName("z") @Serializable(BigDecimalStringSerializer::class) val cumulativeFilledQuantity: BigDecimal,
        @SerialName("L") @Serializable(BigDecimalStringSerializer::class) val lastExecutedPrice: BigDecimal,
        @SerialName("n") @Serializable(BigDecimalStringSerializer::class) val commissionAmount: BigDecimal,
        @SerialName("N") val commissionAsset: String? = null,
        @SerialName("T") @Serializable(InstantLongSerializer::class) val transactionTime: Instant,
        @SerialName("t") val tradeId: Long,
        @SerialName("I") val ignore0: Long,
        @SerialName("w") val inBook: Boolean,
        @SerialName("m") val makerSide: Boolean,
        @SerialName("M") val ignore1: Boolean,
        @SerialName("O") @Serializable(InstantLongSerializer::class) val orderCreationTime: Instant,
        @SerialName("Z") @Serializable(BigDecimalStringSerializer::class) val cumulativeQuoteAssetTransactedQuantity: BigDecimal,
        @SerialName("Y") @Serializable(BigDecimalStringSerializer::class) val lastQuoteAssetTransactedQuantity: BigDecimal,
        @SerialName("Q") @Serializable(BigDecimalStringSerializer::class) val quoteOrderQty: BigDecimal
    ) : AccountEvent

    @Serializable
    data class ListStatusEvent(
        @SerialName("e") val eventType: String,
        @SerialName("E") @Serializable(InstantLongSerializer::class) val eventTime: Instant,
        @SerialName("s") val symbol: String,
        @SerialName("g") val orderListId: Long,
        @SerialName("c") val contingencyType: ContingencyType,
        @SerialName("l") val listStatusType: OCOStatus,
        @SerialName("L") val listOrderStatus: OCOOrderStatus,
        @SerialName("r") val listRejectReason: String,
        @SerialName("C") val listClientOrderId: String,
        @SerialName("T") @Serializable(InstantLongSerializer::class) val transactionTime: Instant,
        @SerialName("O") val orders: List<Order>
    ) : AccountEvent {
        @Serializable
        data class Order(
            @SerialName("s") val symbol: String,
            @SerialName("i") val orderId: Long,
            @SerialName("c") val clientOrderId: String
        )
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
        @Serializable(InstantLongSerializer::class) val createTime: Instant = Instant.now()
    )

    @Serializable
    private data class CachedExchangeInfo(
        val exchangeInfo: ExchangeInfo,
        @Serializable(InstantLongSerializer::class) val createTime: Instant = Instant.now()
    )

    @Serializable
    private data class CachedFeeInfo(
        val fee: Map<String, CachedFee>,
        @Serializable(InstantLongSerializer::class) val createTime: Instant = Instant.now()
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

    //region Request Signers
    private interface Signer {
        fun sign(msg: String): String
    }

    private class HmacSha256Signer(key: String) : Signer {
        private val signingKey: SecretKeySpec
        private val macInstance: Mac

        init {
            val algorithm = "HmacSHA256"
            signingKey = SecretKeySpec(key.toByteArray(), algorithm)
            macInstance = Mac.getInstance(algorithm)
            macInstance.init(signingKey)
        }

        override fun sign(msg: String): String {
            val mac = macInstance.clone() as Mac
            return mac.doFinal(msg.toByteArray()).toHexString()
        }
    }

    private class RsaSigner(privateKey: PrivateKey) : Signer {
        private val signatureInstance = Signature.getInstance("SHA256withRSA")

        init {
            signatureInstance.initSign(privateKey)
        }

        override fun sign(msg: String): String {
            val signature = signatureInstance.clone() as Signature
            signature.update(msg.toByteArray())
            return Base64.getEncoder().encodeToString(signature.sign())
        }
    }
    //endregion

    //region Public Extension
    fun <T, R> EventData<T>.newPayload(payload: R? = null) = EventData(payload, subscribed, error)
    //endregion

    //region Private Extensions
    private fun Map<String, String>.toQueryString() = asSequence().map { "${it.key}=${it.value}" }.joinToString("&")
    private fun String.appendToQueryString(key: String, value: String) = "${if (isBlank()) "" else "$this&"}$key=$value"
    private fun String.appendToUri() = if (isBlank()) "" else "?$this"

    private fun Error.toException() = Exception(code, msg)
    private fun WebSocketEvent.Error.toException() = Exception(code, msg)

    private inline fun <reified T> Flow<EventData<AccountEvent>>.filterAccountEvent(): Flow<EventData<T>> {
        return transform<EventData<AccountEvent>, EventData<T>> { event ->
            if (event.payload is T) {
                emit(event.newPayload(event.payload))
            }
        }
    }

    private fun <T> EventData<T>.setPayload(payload: T?) = EventData(payload, subscribed, null)
    private fun <T> EventData<T>.setSubscribed(subscribed: Boolean) = EventData(payload, subscribed, null)
    private fun <T> EventData<T>.setError(error: Throwable?) = EventData<T>(null, false, error)

    private fun TradeFee.Fee.toCachedFee() = CachedFee(maker, taker)
    //endregion

    //region Private HTTP Logic
    private suspend fun <T> callApi(
        method: String,
        httpMethod: HttpMethod,
        params: Map<String, String>,
        requiresApiKey: Boolean,
        requiresSignature: Boolean,
        retType: DeserializationStrategy<T>,
        initLimits: Boolean = false
    ): T {
        throwIfViolatesLimits()

        var paramsStr = ""
        if (params.isNotEmpty()) {
            paramsStr = params.toQueryString()
            if (requiresSignature) {
                if (signer == null) throw RuntimeException("API key and secret is required to make authenticated calls")
                val signature = signer.sign(paramsStr)
                paramsStr = paramsStr.appendToQueryString(SIGNATURE, signature)
            }
            paramsStr = paramsStr.appendToUri()
        }
        var request = webClient.method(httpMethod).uri("$apiUrl$method$paramsStr")
        if (requiresApiKey) {
            if (apiKey == null) throw RuntimeException("API key is required to make authenticated calls")
            request = request.header(API_KEY_HEADER, apiKey)
        }

        val response = request.awaitExchange()
        val data = response.awaitBody<String>()

        if (!initLimits) response.extractAndCacheLimits()

        if (response.statusCode().is2xxSuccessful) {
            return json.parse(retType, data)
        } else {
            response.extractAndCacheRetryTime()
            val error = json.parse<ErrorMsg>(data)
            throw Exception(error.code, error.msg)
        }
    }

    private tailrec fun throwIfViolatesLimits() {
        val limitViolationInfo = limitViolation.get() ?: return
        val nowMs = System.currentTimeMillis()
        val endTimeMs = limitViolationInfo.endTimeMs
        if (nowMs < endTimeMs) throw Error.TOO_MANY_REQUESTS.toException()
        val set = limitViolation.compareAndSet(limitViolationInfo, null)
        if (!set) throwIfViolatesLimits()
    }

    private suspend fun ClientResponse.extractAndCacheLimits() {
        for (rateLimit in exchangeInfoCache.first().rateLimits) {
            val value = headers().header(rateLimit.httpHeader).firstOrNull()?.toLongOrNull() ?: continue
            reqLimitsCache[rateLimit.httpHeader] = value
        }
    }

    private fun ClientResponse.extractAndCacheRetryTime() {
        if (statusCode() != HttpStatus.TOO_MANY_REQUESTS && statusCode() != HttpStatus.I_AM_A_TEAPOT) return
        val retryAfterSec = headers().header(HttpHeaders.RETRY_AFTER).firstOrNull()?.toLongOrNull() ?: return
        val nowMs = System.currentTimeMillis()
        limitViolation.set(LimitViolation(retryAfterSec * 1000, nowMs))
        if (logger.isDebugEnabled) logger.debug("Limits have been violated. Retry after $retryAfterSec seconds")
    }
    //endregion

    //region WebSocket Logic
    private val connection: Flow<ConnectionData> = run {
        channelFlow<ConnectionData> connection@{
            logger.debug("Starting Binance connection channel")

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
                                                    json.parse<WebSocketEvent.Push>(payloadJsonString)
                                                } catch (e: SerializationException) {
                                                    try {
                                                        json.parse<WebSocketEvent.Response>(payloadJsonString)
                                                    } catch (e: SerializationException) {
                                                        val error = json.parse<WebSocketEvent.Error>(payloadJsonString)
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

                                        val groupedRequests = reqQueue.asSequence()
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
                                                val jsonStr = json.stringify(request)
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

            logger.debug("Closing Binance connection channel")
        }
            .share(1)
            .filter { !it.isClosed() }
    }

    private fun <T : Any> subscribeTo(
        channel: String,
        payloadType: DeserializationStrategy<T>
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
                                            eventData = eventData.setPayload(json.fromJson(payloadType, msg.data))
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
                                                    eventData = eventData.setPayload(json.fromJson(payloadType, msg.data))
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
                    json.parse<CachedListenKey>(wsListenKeyJsonString)
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
                        val wsListenKeyJsonString = json.stringify(CachedListenKey(listenKey))
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
                        val wsListenKeyJsonString = json.stringify(CachedListenKey(listenKey))
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

                        val eventType = eventData.payload.jsonObject["e"]?.contentOrNull

                        val accountEvent: AccountEvent? = try {
                            when (eventType) {
                                "outboundAccountInfo" -> json.fromJson<AccountUpdateEvent>(eventData.payload)
                                "outboundAccountPosition" -> json.fromJson<OutboundAccountPositionEvent>(eventData.payload)
                                "balanceUpdate" -> json.fromJson<BalanceUpdateEvent>(eventData.payload)
                                "executionReport" -> json.fromJson<OrderUpdateEvent>(eventData.payload)
                                "listStatus" -> json.fromJson<ListStatusEvent>(eventData.payload)
                                null -> {
                                    logger.warn("Event type is null in private channel ${eventData.payload}")
                                    null
                                }
                                else -> {
                                    logger.debug("Not recognized event received in private channel ${eventData.payload}")
                                    null
                                }
                            }
                        } catch (e: Throwable) {
                            logger.error("Can't parse json: ${e.message} ${eventData.payload}")
                            null
                        }

                        if (accountEvent != null) send(eventData.newPayload(accountEvent))
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                logger.warn("Subscription to private channel was interrupted: ${e.message}")
            }
        }
    }

    private fun <T> subscribeToCached(channel: String, subscribe: (String) -> Flow<T>): Flow<T> {
        // TODO: streamCache is a concurrentMap. We add values to streamCache but don't remove them. Need to remove them when are not used.
        @Suppress("UNCHECKED_CAST")
        return streamCache.getOrPut(channel) { subscribe(channel).share() } as Flow<T>
    }
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

    //region HTTP and WebSocket Clients
    private fun defaultHttpClient(
        connectTimeoutMs: Long? = 5000,
        readTimeoutMs: Long? = 5000,
        writeTimeoutMs: Long? = 5000
    ): HttpClient {
        val sslContextBuilder = SslContextBuilder.forClient()

        if (System.getenv("HTTP_CERT_TRUST_ALL") != null) {
            sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE)
        }

        val proxyOptions = fun(opt: ProxyProvider.TypeSpec) {
            val tpe = when (System.getenv("HTTP_PROXY_TYPE")) {
                "http" -> ProxyProvider.Proxy.HTTP
                "socks5" -> ProxyProvider.Proxy.SOCKS5
                else -> throw RuntimeException("Can't recognize HTTP_PROXY_TYPE option")
            }

            val host = System.getenv("HTTP_PROXY_HOST")
                ?: throw Exception("Please define HTTP_PROXY_HOST env variable")

            val port = System.getenv("HTTP_PROXY_PORT")?.toInt()
                ?: throw Exception("Please define valid port number for HTTP_PROXY_PORT env variable")

            opt.type(tpe).host(host).port(port)
        }

        return HttpClient.create()
            .headers { it[HttpHeaderNames.USER_AGENT] = "trading-robot" }
            .secure { it.sslContext(sslContextBuilder.build()) }
            .tcpConfiguration { tcpClient ->
                var client = tcpClient
                if (connectTimeoutMs != null) client = client.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs.toInt())
                if (System.getenv("HTTP_PROXY_ENABLED") != null) client = client.proxy(proxyOptions)
                client = client.doOnConnected { conn ->
                    if (readTimeoutMs != null) conn.addHandlerLast(ReadTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS))
                    if (writeTimeoutMs != null) conn.addHandlerLast(WriteTimeoutHandler(writeTimeoutMs, TimeUnit.MILLISECONDS))
                }
                client
            }
    }

    private fun createHttpClient(): WebClient {
        return WebClient.builder()
            .clientConnector(ReactorClientHttpConnector(defaultHttpClient()))
            .codecs {
                it.defaultCodecs().maxInMemorySize(2 * 1024 * 1024)
            }
            .build()
    }

    private fun createWebsocketClient(): WebSocketClient {
        val webSocketClient = ReactorNettyWebSocketClient(defaultHttpClient())
        webSocketClient.maxFramePayloadLength = 65536 * 4
        return webSocketClient
    }
    //endregion

    //region Utilities
    private inline fun ignoreErrors(body: () -> Unit) {
        try {
            body()
        } catch (e: Throwable) {
        }
    }

    private inline fun <reified T : Any> type() = T::class.serializer()
    //endregion

    companion object {
        //region Constants
        private const val API_KEY_HEADER = "X-MBX-APIKEY"
        private const val SIGNATURE = "signature"
        private val LISTEN_KEY_PING_INTERVAL = 45.minutes
        private val FILE_OPTIONS = arrayOf(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
        //endregion

        //region Serializers
        @Serializer(BigDecimal::class)
        private object BigDecimalStringSerializer : KSerializer<BigDecimal> {
            override val descriptor: SerialDescriptor = PrimitiveDescriptor("BinanceBigDecimalStringSerializer", PrimitiveKind.STRING)

            override fun deserialize(decoder: Decoder): BigDecimal {
                return BigDecimal(decoder.decodeString())
            }

            override fun serialize(encoder: Encoder, value: BigDecimal) {
                encoder.encodeString(value.toPlainString())
            }
        }

        @Serializer(BigDecimal::class)
        private object BigDecimalDoubleSerializer : KSerializer<BigDecimal> {
            override val descriptor: SerialDescriptor = PrimitiveDescriptor("BinanceBigDecimalDoubleSerializer", PrimitiveKind.DOUBLE)

            override fun deserialize(decoder: Decoder): BigDecimal {
                return BigDecimal(decoder.decodeDouble().toString())
            }

            override fun serialize(encoder: Encoder, value: BigDecimal) {
                encoder.encodeDouble(value.toDouble())
            }
        }

        @Serializer(Instant::class)
        private object InstantLongSerializer : KSerializer<Instant> {
            override val descriptor: SerialDescriptor = PrimitiveDescriptor("BinanceInstantLongSerializer", PrimitiveKind.LONG)

            override fun deserialize(decoder: Decoder): Instant {
                return Instant.ofEpochMilli(decoder.decodeLong())
            }

            override fun serialize(encoder: Encoder, value: Instant) {
                encoder.encodeLong(value.toEpochMilli())
            }
        }
        //endregion
    }
}
