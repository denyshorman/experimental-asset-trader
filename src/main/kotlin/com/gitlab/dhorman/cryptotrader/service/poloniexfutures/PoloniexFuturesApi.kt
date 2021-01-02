package com.gitlab.dhorman.cryptotrader.service.poloniexfutures

import com.gitlab.dhorman.cryptotrader.util.ignoreErrors
import com.gitlab.dhorman.cryptotrader.util.serializer.*
import com.gitlab.dhorman.cryptotrader.util.signer.HmacSha256Signer
import com.gitlab.dhorman.cryptotrader.util.springWebClient
import com.gitlab.dhorman.cryptotrader.util.springWebsocketClient
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
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set
import kotlin.time.minutes
import kotlin.time.seconds

open class PoloniexFuturesApi(
    private val apiKey: String,
    apiSecret: String,
    private val apiPassphrase: String,
) {
    private val logger = KotlinLogging.logger {}
    private val signer = HmacSha256Signer(apiSecret) { Base64.getEncoder().encodeToString(this) }

    private val webClient = springWebClient(
        connectTimeoutMs = 5000,
        readTimeoutMs = 5000,
        writeTimeoutMs = 5000,
        maxInMemorySize = 2 * 1024 * 1024,
    )

    private val webSocketClient = springWebsocketClient(
        connectTimeoutMs = 5000,
        readTimeoutMs = 60000,
        writeTimeoutMs = 5000,
        maxFramePayloadLength = 65536 * 4,
    )

    private val json = Json {
        ignoreUnknownKeys = true
    }

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("PoloniexFuturesApi"))
    private val closed = AtomicBoolean(false)
    private val streamCache = ConcurrentHashMap<String, Flow<EventData<*>>>()

    init {
        Runtime.getRuntime().addShutdownHook(Thread { runBlocking { close() } })
    }

    //region Maintenance
    suspend fun close() {
        if (closed.getAndSet(true)) return
        scope.coroutineContext[Job]?.cancelAndJoin()
    }
    //endregion

    //region User API
    suspend fun getAccountOverview(currency: String? = null): AccountOverview {
        val params = buildMap<String, String> {
            if (currency != null) put("currency", currency)
        }
        return callApi("/api/v1/account-overview", HttpMethod.GET, params, true, serializer())
    }
    //endregion

    //region Trade API
    //TODO: Implement place order method; Investigate and fix request model
    suspend fun placeOrder(req: PlaceOrderReq): PlaceOrderResp {
        val params = buildMap<String, String> {
            put("clientOid", req.clientOid)
            put("symbol", req.symbol)

            when (req.type) {
                is PlaceOrderReq.Type.Limit -> {
                    put("type", "limit")
                    put("price", req.type.price.toString())

                    put("size", req.type.size.toString())
                    if (req.type.quantity != null) put("quantity", req.type.quantity.toString())
                    if (req.type.postOnly != null) put("postOnly", req.type.postOnly.toString())
                    if (req.type.hidden != null) put("hidden", req.type.hidden.toString())
                    if (req.type.iceberg != null) put("iceberg", req.type.iceberg.toString())
                    if (req.type.visibleSize != null) put("visibleSize", req.type.visibleSize.toString())

                    val timeInForce = when (req.type.timeInForce) {
                        PlaceOrderReq.Type.Limit.TimeInForce.GTC -> "GTC"
                        PlaceOrderReq.Type.Limit.TimeInForce.IOC -> "IOC"
                        null -> null
                    }

                    if (timeInForce != null) put("timeInForce", timeInForce.toString())
                }
                is PlaceOrderReq.Type.Market -> {
                    put("type", "market")
                    if (req.type.size != null) put("size", req.type.size.toString())
                    if (req.type.quantity != null) put("quantity", req.type.quantity.toString())
                }
            }

            if (req.stop != null) {
                val stopType = when (req.stop.type) {
                    PlaceOrderReq.Stop.Type.Down -> "down"
                    PlaceOrderReq.Stop.Type.Up -> "up"
                }

                val stopPriceType = when (req.stop.priceType) {
                    PlaceOrderReq.Stop.PriceType.TradePrice -> "TP"
                    PlaceOrderReq.Stop.PriceType.IndexPrice -> "IP"
                    PlaceOrderReq.Stop.PriceType.MarkPrice -> "MP"
                }

                put("stop", stopType)
                put("stopPriceType", stopPriceType)
                put("stopPrice", req.stop.price.toString())
            }

            when (req.openClose) {
                is PlaceOrderReq.OpenClose.Open -> {
                    put("closeOrder", "false")
                    put("leverage", req.openClose.toString())

                    val side = when (req.openClose.side) {
                        PlaceOrderReq.Side.Buy -> "buy"
                        PlaceOrderReq.Side.Sell -> "sell"
                    }

                    put("side", side)
                }
                PlaceOrderReq.OpenClose.Close -> {
                    put("closeOrder", "true")
                }
            }

            if (req.remark != null) put("remark", req.remark)
            if (req.reduceOnly != null) put("reduceOnly", req.reduceOnly.toString())
            if (req.forceHold != null) put("forceHold", req.forceHold.toString())
        }

        return callApi("/api/v1/orders", HttpMethod.POST, params, true, serializer())
    }
    //endregion

    //region Market Data API
    suspend fun getOpenContracts(): JsonObject {
        return callApi("/api/v1/contracts/active", HttpMethod.GET, emptyMap(), false, serializer())
    }
    //endregion

    //region WebSocket Token API
    suspend fun getPublicToken(): PublicPrivateWsChannelInfo {
        return callApi("/api/v1/bullet-public", HttpMethod.POST, emptyMap(), false, serializer())
    }

    suspend fun getPrivateToken(): PublicPrivateWsChannelInfo {
        return callApi("/api/v1/bullet-private", HttpMethod.POST, emptyMap(), true, serializer())
    }
    //endregion

    //region Market Streams API
    fun tickerStream(symbol: String): Flow<EventData<TickerEvent>> {
        return cacheStream(channel = "/contractMarket/ticker:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "ticker" to TickerEvent.serializer(),
                ),
            )
        }
    }

    fun level2OrderBookStream(symbol: String): Flow<EventData<Level2OrderBookEvent>> {
        return cacheStream(channel = "/contractMarket/level2:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "level2" to Level2OrderBookEvent.serializer(),
                ),
            )
        }
    }

    fun executionStream(symbol: String): Flow<EventData<ExecutionEvent>> {
        return cacheStream(channel = "/contractMarket/execution:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "match" to ExecutionEvent.serializer(),
                ),
            )
        }
    }

    fun level3OrdersTradesStream(symbol: String): Flow<EventData<JsonElement>> {
        return cacheStream(channel = "/contractMarket/level3v2:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "received" to JsonElement.serializer(),
                    "open" to JsonElement.serializer(),
                    "update" to JsonElement.serializer(),
                    "match" to JsonElement.serializer(),
                    "done" to JsonElement.serializer(),
                ),
            )
        }
    }

    fun level2Depth5Stream(symbol: String): Flow<EventData<Level2DepthEvent>> {
        return cacheStream(channel = "/contractMarket/level2Depth5:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "level2" to Level2DepthEvent.serializer(),
                ),
            )
        }
    }

    fun level2Depth50Stream(symbol: String): Flow<EventData<Level2DepthEvent>> {
        return cacheStream(channel = "/contractMarket/level2Depth50:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "level2" to Level2DepthEvent.serializer(),
                ),
            )
        }
    }

    fun contractMarketDataStream(symbol: String): Flow<EventData<MarketDataEvent>> {
        return cacheStream(channel = "/contract/instrument:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "mark.index.price" to MarketDataEvent.MarkIndexPriceEvent.serializer(),
                    "funding.rate" to MarketDataEvent.FundingRateEvent.serializer(),
                ),
            )
        }
    }

    val announcementStream: Flow<EventData<JsonElement>> = run {
        subscribeTo(
            channel = "/contract/announcement",
            mapOf(
                "funding.begin" to JsonElement.serializer(),
                "funding.end" to JsonElement.serializer(),
            ),
        ).shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    fun tranStatsStream(symbol: String): Flow<EventData<StatsEvent>> {
        return cacheStream(channel = "/contractMarket/snapshot:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "snapshot.24h" to StatsEvent.serializer(),
                ),
            )
        }
    }
    //endregion

    //region User Stream API
    val privateMessagesStream: Flow<EventData<JsonElement>> = run {
        subscribeTo(
            channel = "/contractMarket/tradeOrders",
            mapOf(
                "orderChange" to JsonElement.serializer(),
            ),
        ).shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    val advancedOrdersStream: Flow<EventData<JsonElement>> = run {
        subscribeTo(
            channel = "/contractMarket/advancedOrders",
            mapOf(
                "stopOrder" to JsonElement.serializer(),
            ),
        ).shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    val walletStream: Flow<EventData<JsonElement>> = run {
        subscribeTo(
            channel = "/contractAccount/wallet",
            mapOf(
                "orderMargin.change" to JsonElement.serializer(),
                "availableBalance.change" to JsonElement.serializer(),
            ),
        ).shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
    }

    fun positionChangesStream(symbol: String): Flow<EventData<JsonElement>> {
        return cacheStream(channel = "/contract/position:$symbol") { channel ->
            subscribeTo(
                channel,
                mapOf(
                    "position.change" to JsonElement.serializer(),
                    "position.change" to JsonElement.serializer(), //TODO: Looks like 2 data sets in one subject
                    "position.settlement" to JsonElement.serializer(),
                ),
            )
        }
    }
    //endregion

    //region Public Models
    @Serializable
    data class AccountOverview(
        @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedPNL: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val frozenFunds: BigDecimal,
        val currency: String,
        @Serializable(BigDecimalAsDoubleSerializer::class) val accountEquity: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val positionMargin: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val orderMargin: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val marginBalance: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val availableBalance: BigDecimal,
    )

    data class PlaceOrderReq(
        val clientOid: String,
        val type: Type,
        val openClose: OpenClose,
        val symbol: String,
        val remark: String? = null,
        val stop: Stop? = null,
        val reduceOnly: Boolean? = null,
        val forceHold: Boolean? = null,
    ) {
        enum class Side { Buy, Sell }

        data class Stop(
            val type: Type,
            val price: BigDecimal,
            val priceType: PriceType,
        ) {
            enum class Type { Down, Up }
            enum class PriceType { TradePrice, IndexPrice, MarkPrice }
        }

        sealed class OpenClose {
            data class Open(
                val side: Side,
                val leverage: BigDecimal,
            ) : OpenClose()

            object Close : OpenClose()
        }

        sealed class Type {
            data class Limit(
                val price: BigDecimal,
                val size: Int,
                val quantity: BigDecimal? = null,
                val timeInForce: TimeInForce? = null,
                val postOnly: Boolean? = null,
                val hidden: Boolean? = null,
                val iceberg: Boolean? = null,
                val visibleSize: Int? = null,
            ) : Type() {
                enum class TimeInForce { GTC, IOC }
            }

            data class Market(
                val size: Int? = null,
                val quantity: Int? = null,
            ) : Type()
        }
    }

    @Serializable
    data class PlaceOrderResp(
        val orderId: String,
    )

    @Serializable
    data class PublicPrivateWsChannelInfo(
        val instanceServers: List<Server>,
        val token: String,
    ) {
        @Serializable
        data class Server(
            val endpoint: String,
            val pingInterval: Long,
            val pingTimeout: Long,
            val encrypt: Boolean,
            val protocol: String,
        )
    }
    //endregion

    //region Private Models
    private class ConnectionData(
        val requestChannel: Channel<InternalWebSocketRequest> = Channel(Channel.RENDEZVOUS),
        val responseChannelRegistry: ChannelRegistry = ChannelRegistry(),
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
            private val registry = ConcurrentHashMap<String, Channel<WebSocketResponse>>()

            suspend fun register(channelKey: String, channel: Channel<WebSocketResponse>): Boolean {
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

            fun get(channelKey: String): Channel<WebSocketResponse>? {
                return registry[channelKey]
            }

            fun remove(channelKey: String): Channel<WebSocketResponse>? {
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

    private data class InternalWebSocketRequest(
        val request: WebSocketRequest,
        val responseChannel: Channel<WebSocketResponse>,
    )

    @Serializable
    private sealed class WebSocketRequest {
        abstract val id: String

        @Serializable
        @SerialName("ping")
        data class Ping(override val id: String) : WebSocketRequest()

        @Serializable
        @SerialName("subscribe")
        data class Subscribe(
            override val id: String,
            val topic: String,
            val privateChannel: Boolean,
            val response: Boolean,
        ) : WebSocketRequest()

        @Serializable
        @SerialName("unsubscribe")
        data class Unsubscribe(
            override val id: String,
            val topic: String,
            val privateChannel: Boolean,
            val response: Boolean,
        ) : WebSocketRequest()

        @Serializable
        @SerialName("openTunnel")
        data class OpenTunnel(
            override val id: String,
            val newTunnelId: String,
            val response: Boolean,
        ) : WebSocketRequest()
    }

    @Serializable
    private sealed class WebSocketResponse {
        @Serializable
        @SerialName("welcome")
        data class Welcome(val id: String) : WebSocketResponse()

        @Serializable
        @SerialName("pong")
        data class Pong(val id: String) : WebSocketResponse()

        @Serializable
        @SerialName("error")
        data class Error(
            val id: String,
            val code: Int,
            val data: String,
        ) : WebSocketResponse()

        @Serializable
        @SerialName("ack")
        data class Ack(val id: String) : WebSocketResponse()

        @Serializable
        @SerialName("message")
        data class Message(
            val subject: String,
            val topic: String,
            val channelType: String? = null,
            val data: JsonElement,
        ) : WebSocketResponse()
    }

    @Serializable
    private data class HttpResp<T>(val code: String, val data: T)

    @Serializable
    data class HttpErrorResp(val code: String, val msg: String)

    //endregion

    //region Public Events
    data class EventData<T>(
        val payload: T? = null,
        val subscribed: Boolean = false,
        val error: Throwable? = null
    )

    @Serializable
    data class TickerEvent(
        val symbol: String,
        val sequence: Long,
        val side: Side,
        @Serializable(BigDecimalAsDoubleSerializer::class) val size: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val bestBidSize: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val bestAskSize: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val price: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val bestBidPrice: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val bestAskPrice: BigDecimal,
        val tradeId: String,
        @Serializable(InstantAsLongNanoSerializer::class) val ts: Instant,
    ) {
        @Serializable
        enum class Side {
            @SerialName("buy")
            Buy,

            @SerialName("sell")
            Sell,
        }
    }

    @Serializable
    data class Level2OrderBookEvent(
        val sequence: Long,
        val change: Change,
        @Serializable(InstantAsLongMillisSerializer::class) val timestamp: Instant,
    ) {
        @Serializable(Change.Serializer::class)
        data class Change(
            val price: BigDecimal,
            val side: Side,
            val quantity: BigDecimal,
        ) {
            enum class Side { Buy, Sell }

            object Serializer : KSerializer<Change> {
                override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("PoloniexFuturesLevel2OrderBookChange", PrimitiveKind.STRING)

                override fun serialize(encoder: Encoder, value: Change) {
                    encoder.encodeString("${value.price},${value.side.encode()},${value.quantity}")
                }

                override fun deserialize(decoder: Decoder): Change {
                    val string = decoder.decodeString()
                    val change = string.split(",")
                    if (change.size != 3) throw SerializationException("change $change can't be decoded")
                    return Change(
                        BigDecimal(change[0]),
                        change[1].decodeSide(),
                        BigDecimal(change[2]),
                    )
                }

                private fun Side.encode(): String {
                    return when (this) {
                        Side.Buy -> "buy"
                        Side.Sell -> "sell"
                    }
                }

                private fun String.decodeSide(): Side {
                    return when (this) {
                        "buy" -> Side.Buy
                        "sell" -> Side.Sell
                        else -> throw SerializationException("Unknown side $this")
                    }
                }
            }
        }
    }

    @Serializable
    data class ExecutionEvent(
        val makerUserId: String,
        val symbol: String,
        val sequence: Long,
        val side: Side,
        @Serializable(BigDecimalAsDoubleSerializer::class) val size: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val price: BigDecimal,
        val takerOrderId: String,
        val makerOrderId: String,
        val takerUserId: String,
        val tradeId: String,
        @Serializable(InstantAsLongNanoSerializer::class) val ts: Instant,
    ) {
        @Serializable
        enum class Side {
            @SerialName("buy")
            Buy,

            @SerialName("sell")
            Sell,
        }
    }

    @Serializable
    sealed class MarketDataEvent {
        @Serializable
        data class MarkIndexPriceEvent(
            @Serializable(BigDecimalAsDoubleSerializer::class) val granularity: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val indexPrice: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val markPrice: BigDecimal,
            @Serializable(InstantAsLongMillisSerializer::class) val timestamp: Instant,
        ) : MarketDataEvent()

        @Serializable
        data class FundingRateEvent(
            @Serializable(BigDecimalAsDoubleSerializer::class) val granularity: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val fundingRate: BigDecimal,
            @Serializable(InstantAsLongMillisSerializer::class) val timestamp: Instant,
        ) : MarketDataEvent()
    }

    @Serializable
    data class Level2DepthEvent(
        val asks: List<@Serializable(Record.RecordDeserializer::class) Record>,
        val bids: List<@Serializable(Record.RecordDeserializer::class) Record>,
        @Serializable(InstantAsLongMillisSerializer::class) val timestamp: Instant,
    ) {
        data class Record(
            val price: BigDecimal,
            val qty: BigDecimal,
        ) {
            object RecordDeserializer : KSerializer<Record> {
                override val descriptor: SerialDescriptor = buildSerialDescriptor("PoloniexFuturesOrderBookRecordDeserializer", StructureKind.LIST) {
                    element("price", BigDecimalAsDoubleSerializer.descriptor)
                    element("qty", BigDecimalAsDoubleSerializer.descriptor)
                }

                override fun deserialize(decoder: Decoder): Record {
                    return decoder.decodeStructure(descriptor) {
                        var price: BigDecimal? = null
                        var qty: BigDecimal? = null

                        loop@ while (true) {
                            when (val index = decodeElementIndex(descriptor)) {
                                CompositeDecoder.DECODE_DONE -> break@loop
                                0 -> price = decodeSerializableElement(descriptor, index, BigDecimalAsDoubleSerializer)
                                1 -> qty = decodeSerializableElement(descriptor, index, BigDecimalAsDoubleSerializer)
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
    data class StatsEvent(
        @Serializable(BigDecimalAsDoubleSerializer::class) val volume: BigDecimal,
        val symbol: String,
        @Serializable(BigDecimalAsDoubleSerializer::class) val priceChgPct: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val lowPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val highPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val turnover: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val lastPrice: BigDecimal,
        @Serializable(InstantAsLongNanoSerializer::class) val ts: Instant,
    )
    //endregion

    //region Exceptions
    class Exception(val code: String, val description: String) : Throwable("$code: $description", null, true, false)

    class DisconnectedException(override val cause: Throwable? = null) : Throwable("WebSocket connection was closed", cause, true, false)

    enum class Error(val code: Long, val msg: String)
    //endregion

    //region Private HTTP Logic
    private suspend fun <T> callApi(
        urlPath: String,
        httpMethod: HttpMethod,
        params: Map<String, String>,
        requiresSignature: Boolean,
        retType: KSerializer<T>,
    ): T {
        val body: String
        val url: String

        when (httpMethod) {
            HttpMethod.GET, HttpMethod.DELETE -> {
                body = ""
                url = urlPath.appendQueryParams(params)
            }
            HttpMethod.PUT, HttpMethod.POST -> {
                body = if (params.isEmpty()) "" else json.encodeToString(params)
                url = urlPath
            }
            else -> {
                throw IllegalArgumentException("HTTP method $httpMethod is not supported")
            }
        }

        var request = webClient.method(httpMethod).uri("$API_URL$url")

        if (requiresSignature) {
            val timestamp = Instant.now().toEpochMilli().toString()
            val sign = signer.sign("$timestamp$httpMethod$url$body")

            request = request
                .header(PF_API_KEY, apiKey)
                .header(PF_API_SIGN, sign)
                .header(PF_API_TIMESTAMP, timestamp)
                .header(PF_API_PASSPHRASE, apiPassphrase)
        }

        return request.awaitExchange { response ->
            val data = response.awaitBody<String>()

            if (response.statusCode().is2xxSuccessful) {
                val resp = json.decodeFromString(HttpResp.serializer(retType), data)
                resp.data!!
            } else {
                val error = json.decodeFromString<HttpErrorResp>(data)
                throw error.toException()
            }
        }
    }
    //endregion

    //region WebSocket Logic
    private val connection: Flow<ConnectionData> = run {
        channelFlow<ConnectionData> connection@{
            logger.debug("Starting Poloniex Futures connection channel")

            while (isActive) {
                try {
                    var connectionData: ConnectionData? = null
                    val server: PublicPrivateWsChannelInfo.Server
                    val connectUrl: String

                    try {
                        val wsInfo = getPrivateToken()
                        server = wsInfo.instanceServers.firstOrNull()
                            ?: throw Exception("Returned websocket server list is empty")
                        connectUrl = "${server.endpoint}?token=${wsInfo.token}&acceptUserMessage=true"
                    } catch (e: Throwable) {
                        throw DisconnectedException(e)
                    }

                    logger.debug("Establishing connection with ${server.endpoint}...")

                    val session = webSocketClient.execute(URI.create(connectUrl)) { session ->
                        mono(Dispatchers.Unconfined) {
                            logger.info("Connection established with ${server.endpoint}")

                            coroutineScope {
                                val wsMsgReceiver = Channel<WebSocketMessage>(Channel.RENDEZVOUS)
                                val requestResponses = ConcurrentHashMap<String, Channel<WebSocketResponse>>()
                                connectionData = ConnectionData()

                                this@connection.send(connectionData!!)

                                // Messages consumer
                                launch(start = CoroutineStart.UNDISPATCHED) {
                                    session.receive().asFlow()
                                        .filter { it.type == WebSocketMessage.Type.TEXT }
                                        .collect { msg ->
                                            val payloadJsonString = msg.payloadAsText
                                            if (logger.isTraceEnabled) logger.trace("Received: $payloadJsonString")

                                            val event = try {
                                                json.decodeFromString<WebSocketResponse>(payloadJsonString)
                                            } catch (e: Throwable) {
                                                logger.error("Can't handle websocket message: ${e.message}. Payload: $payloadJsonString")
                                                return@collect
                                            }

                                            when (event) {
                                                is WebSocketResponse.Message -> ignoreErrors { connectionData!!.responseChannelRegistry.get(event.topic)?.send(event) }
                                                is WebSocketResponse.Ack -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
                                                is WebSocketResponse.Pong -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
                                                is WebSocketResponse.Error -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
                                                is WebSocketResponse.Welcome -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
                                            }
                                        }

                                    throw ClosedChannelException()
                                }

                                // Message sender
                                launch(start = CoroutineStart.UNDISPATCHED) {
                                    val output = flux(Dispatchers.Unconfined) {
                                        for (msg in wsMsgReceiver) {
                                            send(msg)
                                        }
                                    }
                                    session.send(output).awaitFirstOrNull()
                                    throw ClosedChannelException()
                                }

                                // TODO: Implement ping that is described in spec
                                // Ping requests producer
                                launch {
                                    while (isActive) {
                                        delay(server.pingInterval)
                                        val pingMsg = session.pingMessage { it.wrap("ping".toByteArray()) }
                                        wsMsgReceiver.send(pingMsg)
                                    }
                                }

                                // Request messages aggregator
                                launch {
                                    while (isActive) {
                                        val internalRequest = connectionData!!.requestChannel.receive()
                                        requestResponses[internalRequest.request.id] = internalRequest.responseChannel
                                        val jsonStr = json.encodeToString(internalRequest.request)
                                        val webSocketMsg = session.textMessage(jsonStr)
                                        wsMsgReceiver.send(webSocketMsg)
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
                    logger.info("Connection closed with Poloniex server")
                }
            }

            logger.debug("Closing Poloniex Futures connection channel")
        }
            .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 1)
            .filter { !it.isClosed() }
    }

    private fun <T : R, R> subscribeTo(
        channel: String,
        subjectDeserializers: Map<String, DeserializationStrategy<out T>>,
    ): Flow<EventData<R>> = channelFlow {
        connection.conflate().collect { connection ->
            var state = SubscriptionState.INIT
            var eventData = EventData<R>()

            while (true) {
                when (state) {
                    SubscriptionState.INIT -> {
                        try {
                            withContext(NonCancellable) {
                                val registered = connection.responseChannelRegistry.register(channel, Channel(64))
                                state = if (registered) SubscriptionState.SUBSCRIBE else SubscriptionState.EXIT
                            }
                        } catch (e: CancellationException) {
                            state = SubscriptionState.EXIT
                        }
                    }
                    SubscriptionState.SUBSCRIBE -> {
                        val request = WebSocketRequest.Subscribe(connection.generateId().toString(), channel, privateChannel = false, response = true)
                        val internalRequest = InternalWebSocketRequest(request, connection.responseChannelRegistry.get(channel)!!)
                        state = try {
                            withContext(NonCancellable) {
                                connection.requestChannel.send(internalRequest)
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
                                        connection.responseChannelRegistry.get(channel)!!.receive()
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
                                        is WebSocketResponse.Ack -> {
                                            eventData = eventData.setSubscribed(true)
                                            state = SubscriptionState.CONSUME_EVENTS
                                            if (logger.isDebugEnabled) logger.debug("Subscribed to channel $channel")
                                        }
                                        is WebSocketResponse.Error -> {
                                            eventData = eventData.setError(msg.toException())
                                            state = SubscriptionState.SUBSCRIBE
                                        }
                                        is WebSocketResponse.Message -> {
                                            eventData = eventData.setError(IllegalStateException("Push event was received before confirmation event"))
                                            state = SubscriptionState.UNSUBSCRIBE
                                        }
                                        else -> {
                                            eventData = eventData.setError(IllegalStateException("$msg event was received before confirmation event"))
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
                                for (msg in connection.responseChannelRegistry.get(channel)!!) {
                                    when (msg) {
                                        is WebSocketResponse.Message -> {
                                            val msgDeserializer = subjectDeserializers[msg.subject]
                                            if (msgDeserializer == null) {
                                                logger.debug("No deserializer found for subject ${msg.subject}")
                                                continue
                                            }
                                            val decodedMsg = json.decodeFromJsonElement(msgDeserializer, msg.data)
                                            eventData = eventData.setPayload(decodedMsg)
                                            this@channelFlow.send(eventData)
                                        }
                                        is WebSocketResponse.Error -> {
                                            throw msg.toException()
                                        }
                                        else -> {
                                            throw IllegalStateException("$msg event can't be received during events consumption")
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
                        val request = WebSocketRequest.Unsubscribe(connection.generateId().toString(), channel, privateChannel = true, response = true)
                        val internalRequest = InternalWebSocketRequest(request, connection.responseChannelRegistry.get(channel)!!)
                        state = try {
                            withContext(NonCancellable) {
                                connection.requestChannel.send(internalRequest)
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
                                        for (msg in connection.responseChannelRegistry.get(channel)!!) {
                                            when (msg) {
                                                is WebSocketResponse.Message -> {
                                                    val msgDeserializer = subjectDeserializers[msg.subject]
                                                    if (msgDeserializer == null) {
                                                        logger.debug("No deserializer found for subject ${msg.subject}")
                                                        continue
                                                    }
                                                    val decodedMsg = json.decodeFromJsonElement(msgDeserializer, msg.data)
                                                    eventData = eventData.setPayload(decodedMsg)
                                                    ignoreErrors { this@channelFlow.send(eventData) }
                                                }
                                                is WebSocketResponse.Ack -> {
                                                    eventData = eventData.setSubscribed(false)
                                                    state = SubscriptionState.EXIT
                                                    if (logger.isDebugEnabled) logger.debug("Unsubscribed from channel $channel")
                                                    this@channelFlow.send(eventData)
                                                    return@withTimeout
                                                }
                                                is WebSocketResponse.Error -> {
                                                    throw msg.toException()
                                                }
                                                else -> {
                                                    throw IllegalStateException("$msg event can't be received during unsubscription stage")
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
                        connection.responseChannelRegistry.remove(channel)?.close()
                        return@collect
                    }
                }
            }
        }
    }

    private fun <T> cacheStream(channel: String, subscribe: (String) -> Flow<EventData<T>>): Flow<EventData<T>> {
        @Suppress("UNCHECKED_CAST")
        return streamCache.getOrPut(channel) {
            subscribe(channel)
                .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
        } as Flow<EventData<T>>
    }
    //endregion

    //region Public Extension
    fun <T, R> EventData<T>.newPayload(payload: R? = null) = EventData(payload, subscribed, error)
    //endregion

    //region Private Extensions
    private fun String.appendQueryParams(params: Map<String, String>) = if (params.isEmpty()) this else "$this${params.toQueryString()}"

    private fun Map<String, String>.toQueryString(): String {
        return asSequence()
            .map { (k, v) -> "$k=$v" }
            .joinToString(separator = "&", prefix = "?")
    }

    private fun HttpErrorResp.toException() = Exception(code, msg)
    private fun WebSocketResponse.Error.toException() = Exception(code.toString(), data)

    private fun <T> EventData<T>.setPayload(payload: T?) = EventData(payload, subscribed, null)
    private fun <T> EventData<T>.setSubscribed(subscribed: Boolean) = EventData(payload, subscribed, null)
    private fun <T> EventData<T>.setError(error: Throwable?) = EventData<T>(null, false, error)
    //endregion

    companion object {
        //region Constants
        private const val API_URL = "https://futures-api.poloniex.com"

        private const val PF_API_KEY = "PF-API-KEY"
        private const val PF_API_SIGN = "PF-API-SIGN"
        private const val PF_API_TIMESTAMP = "PF-API-TIMESTAMP"
        private const val PF_API_PASSPHRASE = "PF-API-PASSPHRASE"
        //endregion
    }
}
