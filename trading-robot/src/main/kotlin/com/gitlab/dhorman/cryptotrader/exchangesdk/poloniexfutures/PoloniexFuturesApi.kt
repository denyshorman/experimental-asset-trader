package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexfutures

import com.gitlab.dhorman.cryptotrader.util.*
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
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.BodyInserters
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

class PoloniexFuturesApi(
    apiKey: String,
    apiSecret: String,
    apiPassphrase: String,
) {
    //region Init
    private val json = Json {
        ignoreUnknownKeys = true
    }

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("PoloniexFuturesApi"))

    private val webSocketConnector = WebSocketConnector(this, scope, json)
    private val httpConnector = HttpConnector(apiKey, apiSecret, apiPassphrase, json)
    //endregion

    //region User API
    suspend fun getAccountOverview(currency: String? = null): AccountOverview {
        val params = buildJsonObject {
            if (currency != null) put("currency", currency)
        }
        return httpConnector.callApi("/api/v1/account-overview", HttpMethod.GET, params, true, serializer())
    }
    //endregion

    //region Trade API
    suspend fun placeOrder(req: PlaceOrderReq): PlaceOrderResp {
        val params = buildJsonObject {
            put("clientOid", req.clientOid)
            put("symbol", req.symbol)

            when (req.type) {
                is PlaceOrderReq.Type.Limit -> {
                    put("type", "limit")
                    put("price", req.type.price.toString())

                    when (req.type.amount) {
                        is PlaceOrderReq.Type.Amount.Contract -> put("size", req.type.amount.size)
                        is PlaceOrderReq.Type.Amount.Currency -> put("quantity", req.type.amount.quantity.toString())
                    }

                    if (req.type.postOnly != null) put("postOnly", req.type.postOnly)
                    if (req.type.hidden != null) put("hidden", req.type.hidden)
                    if (req.type.iceberg != null) put("iceberg", req.type.iceberg)
                    if (req.type.visibleSize != null) put("visibleSize", req.type.visibleSize)
                    if (req.type.timeInForce != null) put("timeInForce", req.type.timeInForce.id)
                }
                is PlaceOrderReq.Type.Market -> {
                    put("type", "market")
                    when (req.type.amount) {
                        is PlaceOrderReq.Type.Amount.Contract -> put("size", req.type.amount.size)
                        is PlaceOrderReq.Type.Amount.Currency -> put("quantity", req.type.amount.quantity.toString())
                    }
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
                    put("leverage", req.openClose.leverage.toString())
                    put("side", req.openClose.side.id)
                }
                PlaceOrderReq.OpenClose.Close -> {
                    put("closeOrder", true)
                }
            }

            if (req.remark != null) put("remark", req.remark)
            if (req.reduceOnly != null) put("reduceOnly", req.reduceOnly)
            if (req.forceHold != null) put("forceHold", req.forceHold)
        }

        return httpConnector.callApi("/api/v1/orders", HttpMethod.POST, params, true, serializer())
    }

    suspend fun cancelOrder(orderId: String): CancelOrderResp {
        return httpConnector.callApi("/api/v1/orders/$orderId", HttpMethod.DELETE, emptyJsonObject, true, serializer())
    }

    suspend fun getOrders(req: GetOrdersReq? = null): OrdersResp {
        val params = if (req != null) {
            buildJsonObject {
                if (req.symbol != null) put("symbol", req.symbol)
                if (req.side != null) put("side", req.side.id)
                if (req.type != null) put("type", req.type.id)
                if (req.status != null) put("status", req.status.id)
                if (req.startAt != null) put("startAt", req.startAt.toEpochMilli().toString())
                if (req.endAt != null) put("endtAt", req.endAt.toEpochMilli().toString())
            }
        } else {
            emptyJsonObject
        }
        return httpConnector.callApi("/api/v1/orders", HttpMethod.GET, params, true, serializer())
    }

    suspend fun getPositions(): List<Position> {
        return httpConnector.callApi("/api/v1/positions", HttpMethod.GET, emptyJsonObject, true, serializer())
    }
    //endregion

    //region Market Data API
    suspend fun getOpenContracts(): List<ContractInfo> {
        return httpConnector.callApi("/api/v1/contracts/active", HttpMethod.GET, emptyJsonObject, false, serializer())
    }
    //endregion

    //region WebSocket Token API
    suspend fun getPublicToken(): PublicPrivateWsChannelInfo {
        return httpConnector.callApi("/api/v1/bullet-public", HttpMethod.POST, emptyJsonObject, false, serializer())
    }

    suspend fun getPrivateToken(): PublicPrivateWsChannelInfo {
        return httpConnector.callApi("/api/v1/bullet-private", HttpMethod.POST, emptyJsonObject, true, serializer())
    }
    //endregion

    //region Market Streams API
    fun tickerStream(symbol: String): Flow<EventData<TickerEvent>> {
        return webSocketConnector.subscribeTo(
            "/contractMarket/ticker:$symbol",
            mapOf(
                "ticker" to TickerEvent.serializer(),
            ),
        )
    }

    fun level2OrderBookStream(symbol: String): Flow<EventData<Level2OrderBookEvent>> {
        return webSocketConnector.subscribeTo(
            "/contractMarket/level2:$symbol",
            mapOf(
                "level2" to Level2OrderBookEvent.serializer(),
            ),
        )
    }

    fun executionStream(symbol: String): Flow<EventData<ExecutionEvent>> {
        return webSocketConnector.subscribeTo(
            "/contractMarket/execution:$symbol",
            mapOf(
                "match" to ExecutionEvent.serializer(),
            ),
        )
    }

    fun level3OrdersTradesStream(symbol: String): Flow<EventData<JsonElement>> {
        return webSocketConnector.subscribeTo(
            "/contractMarket/level3v2:$symbol",
            mapOf(
                "received" to JsonElement.serializer(),
                "open" to JsonElement.serializer(),
                "update" to JsonElement.serializer(),
                "match" to JsonElement.serializer(),
                "done" to JsonElement.serializer(),
            ),
        )
    }

    fun level2Depth5Stream(symbol: String): Flow<EventData<Level2DepthEvent>> {
        return webSocketConnector.subscribeTo(
            "/contractMarket/level2Depth5:$symbol",
            mapOf(
                "level2" to Level2DepthEvent.serializer(),
            ),
        )
    }

    fun level2Depth50Stream(symbol: String): Flow<EventData<Level2DepthEvent>> {
        return webSocketConnector.subscribeTo(
            "/contractMarket/level2Depth50:$symbol",
            mapOf(
                "level2" to Level2DepthEvent.serializer(),
            ),
        )
    }

    fun contractMarketDataStream(symbol: String): Flow<EventData<MarketDataEvent>> {
        return webSocketConnector.subscribeTo(
            "/contract/instrument:$symbol",
            mapOf(
                "mark.index.price" to MarketDataEvent.MarkIndexPriceEvent.serializer(),
                "funding.rate" to MarketDataEvent.FundingRateEvent.serializer(),
            ),
        )
    }

    val announcementStream: Flow<EventData<JsonElement>> = run {
        webSocketConnector.subscribeTo(
            "/contract/announcement",
            mapOf(
                "funding.begin" to JsonElement.serializer(),
                "funding.end" to JsonElement.serializer(),
            ),
        )
    }

    fun tranStatsStream(symbol: String): Flow<EventData<StatsEvent>> {
        return webSocketConnector.subscribeTo(
            "/contractMarket/snapshot:$symbol",
            mapOf(
                "snapshot.24h" to StatsEvent.serializer(),
            ),
        )
    }
    //endregion

    //region User Stream API
    val privateMessagesStream: Flow<EventData<PrivateMessageEvent>> = run {
        webSocketConnector.subscribeTo(
            "/contractMarket/tradeOrders",
            mapOf(
                "orderChange" to PrivateMessageEvent.OrderChange.serializer(),
            ),
        )
    }

    val advancedOrdersStream: Flow<EventData<JsonElement>> = run {
        webSocketConnector.subscribeTo(
            "/contractMarket/advancedOrders",
            mapOf(
                "stopOrder" to JsonElement.serializer(),
            ),
        )
    }

    val walletStream: Flow<EventData<AccountBalanceEvent>> = run {
        webSocketConnector.subscribeTo(
            "/contractAccount/wallet",
            mapOf(
                "orderMargin.change" to AccountBalanceEvent.OrderMarginChangeEvent.serializer(),
                "availableBalance.change" to AccountBalanceEvent.AvailableBalanceChangeEvent.serializer(),
            ),
        )
    }

    fun positionChangesStream(symbol: String): Flow<EventData<Any>> {
        return webSocketConnector.subscribeTo(
            "/contract/position:$symbol",
            mapOf(
                "position.change" to PositionEvent.PositionChange.serializer(),
                "position.settlement" to PositionEvent.FundingSettlement.serializer(),
                "openPositionSum.change" to PositionEvent.OpenPositionSumChangeEvent.serializer(),
            ),
        )
    }
    //endregion

    //region Public Models
    @Serializable
    enum class OrderStatus(val id: String) {
        @SerialName("active")
        Active("active"),

        @SerialName("done")
        Done("done"),
    }

    @Serializable
    enum class OrderSide(val id: String) {
        @SerialName("buy")
        Buy("buy"),

        @SerialName("sell")
        Sell("sell"),
    }

    @Serializable
    enum class TimeInForce(val id: String) {
        @SerialName("GTC")
        GoodTillCancel("GTC"),

        @SerialName("IOC")
        ImmediateOrCancel("IOC"),
    }

    @Serializable
    enum class OrderType(val id: String) {
        @SerialName("limit")
        Limit("limit"),

        @SerialName("market")
        Market("market"),

        @SerialName("limit_stop")
        LimitStop("limit_stop"),

        @SerialName("market_stop")
        MarketStop("market_stop"),
    }

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
        val symbol: String,
        val clientOid: String,
        val type: Type,
        val openClose: OpenClose,
        val remark: String? = null,
        val stop: Stop? = null,
        val reduceOnly: Boolean? = null,
        val forceHold: Boolean? = null,
    ) {
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
                val side: OrderSide,
                val leverage: BigDecimal,
            ) : OpenClose()

            object Close : OpenClose()
        }

        sealed class Type {
            data class Limit(
                val price: BigDecimal,
                val amount: Amount,
                val timeInForce: TimeInForce? = null,
                val postOnly: Boolean? = null,
                val hidden: Boolean? = null,
                val iceberg: Boolean? = null,
                val visibleSize: Int? = null,
            ) : Type()

            data class Market(
                val amount: Amount,
            ) : Type()

            sealed class Amount {
                data class Contract(val size: Long) : Amount()
                data class Currency(val quantity: BigDecimal) : Amount()
            }
        }
    }

    @Serializable
    data class PlaceOrderResp(
        val orderId: String,
    )

    @Serializable
    data class CancelOrderResp(
        val cancelFailedOrders: List<FailedOrder>,
        val cancelledOrderIds: List<String>,
    ) {
        @Serializable
        data class FailedOrder(
            @SerialName("orderId") val id: String,
            @SerialName("orderState") val state: Int,
        )
    }

    @Serializable
    data class OrdersResp(
        val totalNum: Int,
        val totalPage: Int,
        val pageSize: Int,
        val currentPage: Int,
        val items: List<Order>,
    ) {
        @Serializable
        data class Order(
            val id: String,
            val clientOid: String,
            val symbol: String,
            val settleCurrency: String,
            val type: OrderType,
            val side: OrderSide,
            val status: OrderStatus,
            val timeInForce: TimeInForce,
            val remark: String,
            @Serializable(BigDecimalAsStringSerializer::class) val leverage: BigDecimal,
            @Serializable(InstantAsLongMillisSerializer::class) val createdAt: Instant,
            @Serializable(InstantAsLongMillisSerializer::class) val updatedAt: Instant,
            @Serializable(InstantAsLongMillisSerializer::class) val endAt: Instant,
            @Serializable(InstantAsLongNanoSerializer::class) val orderTime: Instant,
            @Serializable(BigDecimalAsStringSerializer::class) val price: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val value: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val dealValue: BigDecimal,
            @Serializable(BigDecimalAsStringSerializer::class) val filledValue: BigDecimal,
            val size: Long,
            val dealSize: Long,
            val filledSize: Long,
            // val stp: "",
            // val stop: "",
            // val stopPriceType: "",
            val isActive: Boolean,
            val postOnly: Boolean,
            val hidden: Boolean,
            val reduceOnly: Boolean,
            val forceHold: Boolean,
            val closeOrder: Boolean,
            val iceberg: Boolean,
            val stopTriggered: Boolean,
            val cancelExist: Boolean,
        )
    }

    @Serializable
    data class Position(
        val id: String,
        val symbol: String,
        val settleCurrency: String,
        val isOpen: Boolean,
        val autoDeposit: Boolean,
        val crossMode: Boolean,
        // @Serializable(InstantAsLongMillisSerializer::class) val openingTimestamp: Instant,
        @Serializable(InstantAsLongMillisSerializer::class) val currentTimestamp: Instant,
        @Serializable(BigDecimalAsDoubleSerializer::class) val maintMarginReq: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val riskLimit: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val realLeverage: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val delevPercentage: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val currentQty: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val currentCost: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val currentComm: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedCost: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val realisedGrossCost: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val realisedCost: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val markPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val markValue: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val posCost: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val posCross: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val posInit: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val posComm: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val posLoss: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val posMargin: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val posMaint: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val maintMargin: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val realisedGrossPnl: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val realisedPnl: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedPnl: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedPnlPcnt: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedRoePcnt: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val avgEntryPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val liquidationPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val bankruptPrice: BigDecimal,
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

    @Serializable
    data class GetOrdersReq(
        val symbol: String? = null,
        val side: OrderSide? = null,
        val type: OrderType? = null,
        val status: OrderStatus? = null,
        @Serializable(InstantAsLongMillisSerializer::class) val startAt: Instant? = null,
        @Serializable(InstantAsLongMillisSerializer::class) val endAt: Instant? = null,
    )

    @Serializable
    data class ContractInfo(
        val symbol: String,
        val rootSymbol: String,
        val indexSymbol: String,
        val fundingBaseSymbol: String,
        val fundingRateSymbol: String,
        val fundingQuoteSymbol: String,
        val baseCurrency: String,
        val quoteCurrency: String,
        val settleCurrency: String,
        val type: String,
        val status: String,
        val markMethod: String,
        val fairMethod: String,
        val isQuanto: Boolean,
        val isDeleverage: Boolean,
        val isInverse: Boolean,
        val nextFundingRateTime: Long,
        @Serializable(BigDecimalAsDoubleSerializer::class) val lowPriceOf24h: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val highPriceOf24h: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val volumeOf24h: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val turnoverOf24h: BigDecimal,
        @Serializable(BigDecimalAsStringSerializer::class) val openInterest: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val takerFixFee: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val makerFixFee: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val takerFeeRate: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val makerFeeRate: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val fundingFeeRate: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val predictedFundingFeeRate: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val riskStep: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val minRiskLimit: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val maxRiskLimit: BigDecimal,
        val lotSize: Long,
        @Serializable(BigDecimalAsDoubleSerializer::class) val indexPriceTickSize: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val markPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val indexPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val maxPrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val lastTradePrice: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("tickSize") val minPriceIncrement: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("multiplier") val minOrderQty: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val maxOrderQty: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val maxLeverage: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val initialMargin: BigDecimal,
        @Serializable(BigDecimalAsDoubleSerializer::class) val maintainMargin: BigDecimal,
        @Serializable(InstantAsLongMillisSerializer::class) val firstOpenDate: Instant,
    )
    //endregion

    //region Public Events
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

    @Serializable
    sealed class PrivateMessageEvent {
        @Serializable
        data class OrderChange(
            val orderId: String,
            val clientOid: String? = null,
            val tradeId: String? = null,
            val symbol: String,
            val side: OrderSide,
            val orderType: OrderType? = null,
            val status: Status? = null,
            val type: Type,
            val liquidity: Liquidity? = null,
            val size: Long,
            @Serializable(NullableLongAsStringSerializer::class) val matchSize: Long? = null,
            @Serializable(NullableLongAsStringSerializer::class) val remainSize: Long? = null,
            @Serializable(NullableLongAsStringSerializer::class) val filledSize: Long? = null,
            @Serializable(NullableLongAsStringSerializer::class) val canceledSize: Long? = null,
            @Serializable(NullableLongAsStringSerializer::class) val oldSize: Long? = null,
            @Serializable(NullableBigDecimalAsStringSerializer::class) val price: BigDecimal? = null,
            @Serializable(NullableBigDecimalAsStringSerializer::class) val matchPrice: BigDecimal? = null,
            @Serializable(InstantAsLongNanoSerializer::class) val ts: Instant,
            @Serializable(InstantAsLongNanoSerializer::class) val orderTime: Instant,
        ) : PrivateMessageEvent() {
            @Serializable
            enum class Type {
                @SerialName("open")
                Open,

                @SerialName("match")
                Match,

                @SerialName("filled")
                Filled,

                @SerialName("canceled")
                Canceled,

                @SerialName("update")
                Update,
            }

            @Serializable
            enum class Status {
                @SerialName("match")
                Match,

                @SerialName("open")
                Open,

                @SerialName("done")
                Done,
            }

            @Serializable
            enum class Liquidity {
                @SerialName("maker")
                Maker,

                @SerialName("taker")
                Taker,
            }
        }
    }

    @Serializable
    sealed class AccountBalanceEvent {
        @Serializable
        data class AvailableBalanceChangeEvent(
            val currency: String,
            @Serializable(BigDecimalAsStringSerializer::class) val availableBalance: BigDecimal,
            @Serializable(InstantAsLongStringMillisSerializer::class) val timestamp: Instant,
        ) : AccountBalanceEvent()

        @Serializable
        data class OrderMarginChangeEvent(
            val currency: String,
            @Serializable(BigDecimalAsStringSerializer::class) val orderMargin: BigDecimal,
            @Serializable(InstantAsLongStringMillisSerializer::class) val timestamp: Instant,
        ) : AccountBalanceEvent()
    }

    @Serializable
    sealed class PositionEvent {
        @Serializable
        data class PositionChange(
            val symbol: String,
            val settleCurrency: String,
            val crossMode: Boolean? = null,
            val autoDeposit: Boolean? = null,
            val isOpen: Boolean? = null,
            @Serializable(InstantAsLongMillisSerializer::class) val currentTimestamp: Instant,
            @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedPnl: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val realisedPnl: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) val markPrice: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val liquidationPrice: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) val avgEntryPrice: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) val bankruptPrice: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("maintMargin") val maintenanceMargin: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("maintMarginReq") val maintenanceMarginRate: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("realLeverage") val leverage: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedRoePcnt: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedPnlPcnt: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val unrealisedCost: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) val realisedCost: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("realisedGrossCost") val accumulatedRealizedProfit: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) val realisedGrossPnl: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("delevPercentage") val deleveragePercentage: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val markValue: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val riskLimit: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("posInit") val posInit: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("posLoss") val posLoss: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("posMargin") val posMargin: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("posMaint") val posMaintenance: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("posCost") val posCost: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("posComm") val posBankruptcyCost: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("posCross") val posManuallyAddedMargin: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) val currentCost: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) val currentQty: BigDecimal? = null,
            @Serializable(BigDecimalAsDoubleSerializer::class) @SerialName("currentComm") val currentCommission: BigDecimal? = null,
        )

        @Serializable
        data class FundingSettlement(
            val settleCurrency: String,
            @Serializable(BigDecimalAsDoubleSerializer::class) val qty: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val markPrice: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val fundingRate: BigDecimal,
            @Serializable(BigDecimalAsDoubleSerializer::class) val fundingFee: BigDecimal,
            @Serializable(InstantAsLongNanoSerializer::class) val ts: Instant,
            @Serializable(InstantAsLongMillisSerializer::class) val fundingTime: Instant,
        )

        @Serializable
        data class OpenPositionSumChangeEvent(
            val sum: Long,
        ) : PositionEvent()
    }
    //endregion

    //region Exceptions
    class Exception(val code: String, val description: String) : Throwable("$code: $description", null, true, false)

    class DisconnectedException(override val cause: Throwable? = null) : Throwable("WebSocket connection was closed", cause, true, false)

    enum class Error(val code: Long, val msg: String)
    //endregion

    //region Utils
    private class WebSocketConnector(
        val poloniexFuturesApi: PoloniexFuturesApi,
        val scope: CoroutineScope,
        val json: Json,
    ) {
        private val streamCache = ConcurrentHashMap<String, Flow<EventData<*>>>()

        private val webSocketClient = springWebsocketClient(
            connectTimeoutMs = 5000,
            readTimeoutMs = 60000,
            writeTimeoutMs = 5000,
            maxFramePayloadLength = 65536 * 4,
        )

        private val connection: Flow<ConnectionData> = run {
            channelFlow<ConnectionData> connection@{
                logger.debug("Starting WebSocket connection channel")

                while (isActive) {
                    try {
                        var connectionData: ConnectionData? = null
                        val server: PublicPrivateWsChannelInfo.Server
                        val connectUrl: String

                        try {
                            val wsInfo = poloniexFuturesApi.getPrivateToken()
                            server = wsInfo.instanceServers.firstOrNull()
                                ?: throw Exception("Returned websocket server list is empty")
                            connectUrl = "${server.endpoint}?token=${wsInfo.token}&acceptUserMessage=true"
                        } catch (e: Throwable) {
                            throw DisconnectedException(e)
                        }

                        logger.debug("Establishing connection...")

                        val session = webSocketClient.execute(URI.create(connectUrl)) { session ->
                            mono(Dispatchers.Unconfined) {
                                logger.info("Connection established")

                                coroutineScope {
                                    val wsMsgReceiver = Channel<WebSocketMessage>(Channel.RENDEZVOUS)
                                    val requestResponses = ConcurrentHashMap<String, Channel<WebSocketInboundMessage>>()
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
                                                    json.decodeFromString<WebSocketInboundMessage>(payloadJsonString)
                                                } catch (e: Throwable) {
                                                    logger.error("Can't handle websocket message: ${e.message}. Payload: $payloadJsonString")
                                                    return@collect
                                                }

                                                when (event) {
                                                    is WebSocketInboundMessage.Message -> ignoreErrors { connectionData!!.inboundChannelRegistry.get(event.topic)?.send(event) }
                                                    is WebSocketInboundMessage.Notice -> ignoreErrors { connectionData!!.inboundChannelRegistry.get(event.topic)?.send(event) }
                                                    is WebSocketInboundMessage.Ack -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
                                                    is WebSocketInboundMessage.Pong -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
                                                    is WebSocketInboundMessage.Error -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
                                                    is WebSocketInboundMessage.Welcome -> ignoreErrors { requestResponses.remove(event.id)?.send(event) }
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
                                            val internalRequest = connectionData!!.outboundChannel.receive()
                                            requestResponses[internalRequest.outboundMessage.id] = internalRequest.inboundChannel
                                            val jsonStr = json.encodeToString(internalRequest.outboundMessage)
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
                        logger.info("Connection closed")
                    }
                }

                logger.debug("Closing WebSocket connection channel")
            }
                .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 1)
                .filter { !it.isClosed() }
        }

        fun <T : R, R> subscribeTo(
            channel: String,
            subjectDeserializers: Map<String, DeserializationStrategy<out T>>,
        ): Flow<EventData<R>> {
            @Suppress("UNCHECKED_CAST")
            return streamCache.getOrPut(channel) {
                subscribeToImpl(channel, subjectDeserializers)
                    .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 0)
            } as Flow<EventData<T>>
        }

        private fun <T : R, R> subscribeToImpl(
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
                                    val registered = connection.inboundChannelRegistry.register(channel, Channel(64))
                                    state = if (registered) SubscriptionState.SUBSCRIBE else SubscriptionState.EXIT
                                }
                            } catch (e: CancellationException) {
                                state = SubscriptionState.EXIT
                            }
                        }
                        SubscriptionState.SUBSCRIBE -> {
                            val request = WebSocketOutboundMessage.Subscribe(connection.generateId().toString(), channel, privateChannel = false, response = true)
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
                                            is WebSocketInboundMessage.Ack -> {
                                                eventData = eventData.setSubscribed(true)
                                                state = SubscriptionState.CONSUME_EVENTS
                                                if (logger.isDebugEnabled) logger.debug("Subscribed to channel $channel")
                                            }
                                            is WebSocketInboundMessage.Error -> {
                                                eventData = eventData.setError(msg.toException())
                                                state = SubscriptionState.SUBSCRIBE
                                            }
                                            is WebSocketInboundMessage.Message, is WebSocketInboundMessage.Notice -> {
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
                                    for (msg in connection.inboundChannelRegistry.get(channel)!!) {
                                        when (msg) {
                                            is WebSocketInboundMessage.Message -> {
                                                val msgDeserializer = subjectDeserializers[msg.subject]
                                                if (msgDeserializer == null) {
                                                    logger.debug("No deserializer found for subject ${msg.subject} in channel $channel: $msg")
                                                    continue
                                                }
                                                val decodedMsg = json.decodeFromJsonElement(msgDeserializer, msg.data)
                                                eventData = eventData.setPayload(decodedMsg)
                                                this@channelFlow.send(eventData)
                                            }
                                            is WebSocketInboundMessage.Notice -> {
                                                val msgDeserializer = subjectDeserializers[msg.subject]
                                                if (msgDeserializer == null) {
                                                    logger.debug("No deserializer found for subject ${msg.subject} in channel $channel: $msg")
                                                    continue
                                                }
                                                val decodedMsg = json.decodeFromJsonElement(msgDeserializer, msg.data)
                                                eventData = eventData.setPayload(decodedMsg)
                                                this@channelFlow.send(eventData)
                                            }
                                            is WebSocketInboundMessage.Error -> {
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
                            val request = WebSocketOutboundMessage.Unsubscribe(connection.generateId().toString(), channel, privateChannel = true, response = true)
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
                                                    is WebSocketInboundMessage.Message -> {
                                                        val msgDeserializer = subjectDeserializers[msg.subject]
                                                        if (msgDeserializer == null) {
                                                            logger.debug("No deserializer found for subject ${msg.subject} in channel $channel: $msg")
                                                            continue
                                                        }
                                                        val decodedMsg = json.decodeFromJsonElement(msgDeserializer, msg.data)
                                                        eventData = eventData.setPayload(decodedMsg)
                                                        ignoreErrors { this@channelFlow.send(eventData) }
                                                    }
                                                    is WebSocketInboundMessage.Notice -> {
                                                        val msgDeserializer = subjectDeserializers[msg.subject]
                                                        if (msgDeserializer == null) {
                                                            logger.debug("No deserializer found for subject ${msg.subject} in channel $channel: $msg")
                                                            continue
                                                        }
                                                        val decodedMsg = json.decodeFromJsonElement(msgDeserializer, msg.data)
                                                        eventData = eventData.setPayload(decodedMsg)
                                                        ignoreErrors { this@channelFlow.send(eventData) }
                                                    }
                                                    is WebSocketInboundMessage.Ack -> {
                                                        eventData = eventData.setSubscribed(false)
                                                        state = SubscriptionState.EXIT
                                                        if (logger.isDebugEnabled) logger.debug("Unsubscribed from channel $channel")
                                                        this@channelFlow.send(eventData)
                                                        return@withTimeout
                                                    }
                                                    is WebSocketInboundMessage.Error -> {
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
                            connection.inboundChannelRegistry.remove(channel)?.close()
                            return@collect
                        }
                    }
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

        //region Models
        @Serializable
        private sealed class WebSocketOutboundMessage {
            abstract val id: String

            @Serializable
            @SerialName("ping")
            data class Ping(override val id: String) : WebSocketOutboundMessage()

            @Serializable
            @SerialName("subscribe")
            data class Subscribe(
                override val id: String,
                val topic: String,
                val privateChannel: Boolean,
                val response: Boolean,
            ) : WebSocketOutboundMessage()

            @Serializable
            @SerialName("unsubscribe")
            data class Unsubscribe(
                override val id: String,
                val topic: String,
                val privateChannel: Boolean,
                val response: Boolean,
            ) : WebSocketOutboundMessage()

            @Serializable
            @SerialName("openTunnel")
            data class OpenTunnel(
                override val id: String,
                val newTunnelId: String,
                val response: Boolean,
            ) : WebSocketOutboundMessage()
        }

        @Serializable
        private sealed class WebSocketInboundMessage {
            @Serializable
            @SerialName("welcome")
            data class Welcome(val id: String) : WebSocketInboundMessage()

            @Serializable
            @SerialName("pong")
            data class Pong(val id: String) : WebSocketInboundMessage()

            @Serializable
            @SerialName("error")
            data class Error(
                val id: String,
                val code: Int,
                val data: String,
            ) : WebSocketInboundMessage()

            @Serializable
            @SerialName("ack")
            data class Ack(val id: String) : WebSocketInboundMessage()

            @Serializable
            @SerialName("message")
            data class Message(
                val subject: String,
                val topic: String,
                val channelType: String? = null,
                val userId: String? = null,
                val data: JsonElement,
            ) : WebSocketInboundMessage()

            @Serializable
            @SerialName("notice")
            data class Notice(
                val subject: String,
                val topic: String,
                val channelType: String? = null,
                val userId: String? = null,
                val data: JsonElement,
            ) : WebSocketInboundMessage()
        }
        //endregion

        companion object {
            private val logger = KotlinLogging.logger {}

            private fun WebSocketInboundMessage.Error.toException() = Exception(code.toString(), data)
        }
    }

    private class HttpConnector(
        private val apiKey: String,
        apiSecret: String,
        private val apiPassphrase: String,
        private val json: Json,
    ) {
        private val signer = HmacSha256Signer(apiSecret) { Base64.getEncoder().encodeToString(this) }

        private val webClient = springWebClient(
            connectTimeoutMs = 5000,
            readTimeoutMs = 5000,
            writeTimeoutMs = 5000,
            maxInMemorySize = 2 * 1024 * 1024,
        )

        suspend fun <T> callApi(
            urlPath: String,
            httpMethod: HttpMethod,
            params: JsonObject,
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

            return webClient
                .method(httpMethod)
                .uri("$API_URL$url")
                .accept(MediaType.APPLICATION_JSON)
                .run {
                    if (httpMethod == HttpMethod.PUT || httpMethod == HttpMethod.POST) {
                        this
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(BodyInserters.fromValue(body))
                    } else {
                        this
                    }
                }
                .run {
                    if (requiresSignature) {
                        val timestamp = Instant.now().toEpochMilli().toString()
                        val sign = signer.sign("$timestamp$httpMethod$url$body")

                        this
                            .header(PF_API_KEY, apiKey)
                            .header(PF_API_SIGN, sign)
                            .header(PF_API_TIMESTAMP, timestamp)
                            .header(PF_API_PASSPHRASE, apiPassphrase)
                    } else {
                        this
                    }
                }
                .awaitExchange { response ->
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

        @Serializable
        private data class HttpResp<T>(val code: String, val data: T)

        @Serializable
        private data class HttpErrorResp(val code: String, val msg: String)

        companion object {
            //region Constants
            private const val API_URL = "https://futures-api.poloniex.com"

            private const val PF_API_KEY = "PF-API-KEY"
            private const val PF_API_SIGN = "PF-API-SIGN"
            private const val PF_API_TIMESTAMP = "PF-API-TIMESTAMP"
            private const val PF_API_PASSPHRASE = "PF-API-PASSPHRASE"
            //endregion

            //region Extension
            private fun String.appendQueryParams(params: JsonObject) = if (params.isEmpty()) this else "$this${params.toQueryString()}"

            private fun JsonObject.toQueryString(): String {
                return asSequence()
                    .map { (key, v) ->
                        val value = when (v) {
                            is JsonPrimitive -> (v.contentOrNull ?: v.booleanOrNull ?: v.intOrNull ?: v.longOrNull ?: v.doubleOrNull ?: v.floatOrNull).toString()
                            else -> throw RuntimeException("Nested parameters are not supported")
                        }
                        "$key=$value"
                    }
                    .joinToString(separator = "&", prefix = "?")
            }

            private fun HttpErrorResp.toException() = Exception(code, msg)
            //endregion
        }
    }
    //endregion
}
