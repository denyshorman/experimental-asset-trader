package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.core.oneMinusAdjPoloniex
import com.gitlab.dhorman.cryptotrader.core.toMarket
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.util.*
import com.gitlab.dhorman.cryptotrader.util.limiter.RequestLimiter
import com.gitlab.dhorman.cryptotrader.util.signer.HmacSha512Signer
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException
import io.netty.handler.ssl.SslHandshakeTimeoutException
import io.netty.handler.timeout.ReadTimeoutException
import io.netty.handler.timeout.WriteTimeoutException
import io.vavr.Tuple2
import io.vavr.collection.Array
import io.vavr.collection.HashMap
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.flux
import kotlinx.coroutines.reactor.mono
import mu.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.awaitExchange
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.net.ConnectException
import java.net.SocketException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private const val PoloniexPrivatePublicHttpApiUrl = "https://poloniex.com"
private const val PoloniexWebSocketApiUrl = "wss://api2.poloniex.com"

/**
 * Documentation https://docs.poloniex.com
 */
open class PoloniexApi(
    private val poloniexApiKey: String,
    poloniexApiSecret: String,
    private val objectMapper: ObjectMapper,
) {
    private val logger = KotlinLogging.logger {}
    private val signer = HmacSha512Signer(poloniexApiSecret, toHexString)
    private val reqLimiter = RequestLimiter(allowedRequests = 7, perInterval = Duration.ofSeconds(1))

    private val webClient = springWebClient(
        connectTimeoutMs = 5000,
        readTimeoutMs = 5000,
        writeTimeoutMs = 5000,
        maxInMemorySize = 5 * 1024 * 1024,
    )

    private val webSocketClient = springWebsocketClient(
        connectTimeoutMs = 5000,
        readTimeoutMs = 5000,
        writeTimeoutMs = 5000,
        maxFramePayloadLength = 65536 * 4,
    )

    private val channels = ConcurrentHashMap<Int, BroadcastChannel<PushNotification>>()
    private val channelState = ConcurrentHashMap<Int, MutableStateFlow<Boolean>>()
    private val connectionOutput = Channel<String>(Channel.RENDEZVOUS)

    private val defaultChannelState = { MutableStateFlow(false) }

    private fun getChannelState(channel: Int): MutableStateFlow<Boolean> {
        return channelState.getOrPut(channel, defaultChannelState)
    }

    fun channelStateFlow(channel: Int): StateFlow<Boolean> {
        return getChannelState(channel)
    }

    private val pushNotificationJsonReader = objectMapper.readerFor(PushNotification::class.java)

    open val connection: Flow<Boolean> = channelFlow {
        logger.debug("Starting Poloniex connection channel")

        while (isActive) {
            logger.debug("Establishing connection with $PoloniexWebSocketApiUrl...")

            try {
                val session = webSocketClient.execute(URI.create(PoloniexWebSocketApiUrl)) { session ->
                    mono(Dispatchers.Unconfined) {
                        logger.info("Connection established with $PoloniexWebSocketApiUrl")

                        coroutineScope {
                            launch(start = CoroutineStart.UNDISPATCHED) {
                                session.receive().asFlow().onStart { send(true) }.collect { msg ->
                                    val payloadBytes = ByteArray(msg.payload.readableByteCount())
                                    msg.payload.read(payloadBytes)

                                    if (logger.isTraceEnabled) {
                                        val payloadStr = String(payloadBytes, StandardCharsets.UTF_8)
                                        logger.trace("Received: $payloadStr")
                                    }

                                    try {
                                        val payload = pushNotificationJsonReader.readValue<PushNotification>(payloadBytes)
                                        channels[payload.channel]?.send(payload)
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: MismatchedInputException) {
                                        val error: Error
                                        try {
                                            error = objectMapper.readValue(payloadBytes)
                                        } catch (e: Throwable) {
                                            throw e
                                        }

                                        if (error.msg != null) {
                                            if (error.msg == PermissionDeniedMsg) throw PermissionDeniedException
                                            if (error.msg == InvalidChannelMsg) throw InvalidChannelException
                                        }

                                        throw e
                                    } catch (e: Throwable) {
                                        val payloadStr = String(payloadBytes, StandardCharsets.UTF_8)
                                        logger.error("Can't parse websocket message: ${e.message}. Payload: $payloadStr")
                                    }
                                }
                                throw DisconnectedException
                            }

                            launch {
                                val output = flux(Dispatchers.Unconfined) {
                                    for (msgStr in connectionOutput) {
                                        if (logger.isTraceEnabled) logger.trace("Sent: $msgStr")
                                        val webSocketMsg = session.textMessage(msgStr)
                                        send(webSocketMsg)
                                    }
                                }
                                session.send(output).awaitFirstOrNull()
                                throw DisconnectedException
                            }
                        }

                        null
                    }
                }

                try {
                    session.awaitFirstOrNull()
                } finally {
                    send(false)
                }

                logger.debug("Connection stopped without error")
            } catch (e: CancellationException) {
                logger.debug("Connection closed because internal job has been cancelled")
            } catch (e: DisconnectedException) {
                logger.debug(e.message)
                delay(1000)
            } catch (e: ReadTimeoutException) {
                logger.warn("No data was read within a certain period of time from $PoloniexWebSocketApiUrl")
                delay(500)
            } catch (e: WriteTimeoutException) {
                logger.warn("Write operation cannot finish in a certain period of time")
                delay(500)
            } catch (e: InvalidChannelException) {
                logger.error(e.message)
                delay(1000)
            } catch (e: PermissionDeniedException) {
                logger.debug("WebSocket private channel sent permission denied message")
            } catch (e: ConnectException) {
                logger.warn(e.message)
                delay(1000)
            } catch (e: SocketException) {
                logger.warn(e.message)
                delay(1000)
            } catch (e: WebSocketHandshakeException) {
                logger.warn(e.message)
                delay(1000)
            } catch (e: SslHandshakeTimeoutException) {
                logger.warn(e.message)
                delay(1000)
            } catch (e: Throwable) {
                logger.error(e.message)
                delay(1000)
            } finally {
                logger.info("Connection closed with $PoloniexWebSocketApiUrl")
            }
        }

        logger.debug("Closing Poloniex connection channel")
    }.distinctUntilChanged().share(1)

    open val tickerStream: Flow<Ticker> = run {
        subscribeTo(DefaultChannel.TickerData.id, jacksonTypeRef<Ticker>()).conflate().share()
    }

    open val dayExchangeVolumeStream: Flow<DayExchangeVolume> = run {
        subscribeTo(DefaultChannel.DayExchangeVolume.id, jacksonTypeRef<DayExchangeVolume>())
            .share()
    }

    open val accountNotificationStream: Flow<List<AccountNotification>> = run {
        subscribeTo(
            DefaultChannel.AccountNotifications.id,
            jacksonTypeRef<List<AccountNotification>>(),
            privateApi = true
        )
            .onEach { notifications ->
                if (logger.isDebugEnabled) {
                    var tradeNotificationExist = false
                    for (notification in notifications) {
                        if (notification is TradeNotification) {
                            tradeNotificationExist = true
                            break
                        }
                    }

                    if (tradeNotificationExist) {
                        val data = notifications.filter { it is TradeNotification || it is BalanceUpdate }
                        logger.debug("Trade notifications: $data")
                    }
                }
            }
            .share()
    }

    @Throws(InvalidMarketException::class, InvalidDepthException::class)
    open suspend fun orderBooks(market: Market? = null, depth: Int? = null): Map<Market, OrderBookSnapshot> {
        val command = "returnOrderBook"

        val params = hashMap(
            "currencyPair" to (market?.toString() ?: "all"),
            "depth" to (depth?.toString() ?: "30")
        )

        try {
            return if (market == null) {
                callPublicApi(command, jacksonTypeRef(), params)
            } else {
                val orderBookData = callPublicApi(command, jacksonTypeRef<OrderBookSnapshot>(), params)
                hashMap(market to orderBookData)
            }
        } catch (e: Throwable) {
            if (e.message == InvalidMarketMsg) throw InvalidMarketException
            if (e.message == InvalidDepthMsg) throw InvalidDepthException
            throw e
        }
    }

    open suspend fun dayVolume(): Map<Market, Tuple2<Amount, Amount>> {
        return callPublicApi("return24hVolume", jacksonTypeRef<Map<String, Any>>())
            .toVavrStream()
            .filter { it._2 is kotlin.collections.Map<*, *> }
            .map {
                val market = it._1.toMarket()
                val currentAmountMap = it._2 as kotlin.collections.Map<*, *>
                val baseCurrencyAmount = BigDecimal(currentAmountMap[market.baseCurrency] as String)
                val quoteCurrencyAmount = BigDecimal(currentAmountMap[market.quoteCurrency] as String)
                tuple(market, tuple(baseCurrencyAmount, quoteCurrencyAmount))
            }
            .toMap({ it._1 }, { it._2 })
    }

    open suspend fun candlestickChartData(market: Market, fromDate: Instant, toDate: Instant, period: ChartDataCandlestickPeriod): List<Candlestick> {
        val params = hashMap(
            "currencyPair" to market.toString(),
            "start" to fromDate.epochSecond.toString(),
            "end" to toDate.epochSecond.toString(),
            "period" to period.sec.toString()
        )

        return callPublicApi("returnChartData", jacksonTypeRef(), params)
    }

    open suspend fun ticker(): Map<Market, Ticker0> {
        val command = "returnTicker"
        return callPublicApi(command, jacksonTypeRef())
    }

    open suspend fun tradeHistoryPublic(market: Market, fromTs: Instant? = null, toTs: Instant? = null): Array<TradeHistory> {
        val command = "returnTradeHistory"

        val params = hashMap(
            "currencyPair" to some(market.toString()),
            "start" to fromTs.option().map { it.epochSecond.toString() },
            "end" to toTs.option().map { it.epochSecond.toString() }
        )
            .iterator()
            .filter { it._2.isDefined }
            .map { v -> tuple(v._1, v._2.get()) }
            .toMap { it }

        return callPublicApi(command, jacksonTypeRef(), params)
    }

    open suspend fun currencies(): Map<Currency, CurrencyDetails> {
        val command = "returnCurrencies"
        return callPublicApi(command, jacksonTypeRef())
    }

    open suspend fun availableBalances(): Map<Currency, BigDecimal> {
        return callPrivateApi("returnBalances", jacksonTypeRef())
    }

    open suspend fun completeBalances(): Map<Currency, CompleteBalance> {
        return callPrivateApi("returnCompleteBalances", jacksonTypeRef())
    }

    open suspend fun openOrders(market: Market): List<OpenOrder> {
        return callPrivateApi("returnOpenOrders", jacksonTypeRef(), hashMap("currencyPair" to market.toString()))
    }

    open suspend fun allOpenOrders(): Map<Long, OpenOrderWithMarket> {
        val command = "returnOpenOrders"
        val params = hashMap("currencyPair" to "all")
        return callPrivateApi(command, jacksonTypeRef<Map<Market, List<OpenOrder>>>(), params)
            .flatMap { marketOrders ->
                marketOrders._2.map { order -> OpenOrderWithMarket.from(order, marketOrders._1) }
            }
            .toMap({ it.orderId }, { it })
    }

    open suspend fun orderStatus(orderId: Long): OrderStatus? {
        val command = "returnOrderStatus"
        val params = hashMap("orderNumber" to orderId.toString())
        val json = callPrivateApi(command, jacksonTypeRef<JsonNode>(), params)

        try {
            val res = objectMapper.convertValue(json, jacksonTypeRef<OrderStatusWrapper>())
            return if (res.success) {
                res.result.getOrNull(orderId)
            } else {
                null
            }
        } catch (e: Throwable) {
            try {
                val res = objectMapper.convertValue(json, jacksonTypeRef<OrderStatusErrorWrapper>())
                val errorMsg = res.result.getOrNull("error")
                if (logger.isTraceEnabled && errorMsg != null) logger.trace("Can't get order status: $errorMsg")
                return null
            } catch (e: Throwable) {
                throw e
            }
        }
    }

    open suspend fun tradeHistory(market: Market? = null, fromTs: Instant? = null, toTs: Instant? = null, limit: Long? = null): Map<Market, List<TradeHistoryPrivate>> {
        val command = "returnTradeHistory"
        val params = hashMap(
            "currencyPair" to some(market?.toString() ?: "all"),
            "start" to fromTs.option().map { it.epochSecond.toString() },
            "end" to toTs.option().map { it.epochSecond.toString() },
            "limit" to limit.option().map { it.toString() }
        )
            .iterator()
            .filter { it._2.isDefined }
            .map { v -> tuple(v._1, v._2.get()) }
            .toMap { it }

        return if (market == null) {
            callPrivateApi(command, jacksonTypeRef(), params)
        } else {
            val trades = callPrivateApi(command, jacksonTypeRef<List<TradeHistoryPrivate>>(), params)
            hashMap(market to trades)
        }
    }

    open suspend fun orderTrades(orderNumber: Long): List<OrderTrade> {
        val command = "returnOrderTrades"
        val params = hashMap("orderNumber" to orderNumber.toString())
        return try {
            callPrivateApi(command, jacksonTypeRef(), params)
        } catch (e: Throwable) {
            if (e.message == OrderNotFoundPattern) {
                List.empty()
            } else {
                throw e
            }
        }
    }

    open suspend fun placeLimitOrder(
        market: Market,
        orderType: OrderType,
        price: BigDecimal,
        amount: BigDecimal,
        tpe: BuyOrderType?,
        clientOrderId: Long? = null
    ): LimitOrderResult {
        return when (orderType) {
            OrderType.Buy -> placeLimitOrder("buy", market, price, amount, tpe, clientOrderId)
            OrderType.Sell -> placeLimitOrder("sell", market, price, amount, tpe, clientOrderId)
        }
    }

    /**
     * A limit order is one of the most basic order types. It allows the trader to specify a price and amount they would like to buy or sell.
     *
     * Example: If the current market price is 250 and I want to buy lower than that at 249, then I would place a limit buy order at 249. If the market reaches 249 and a seller’s ask matches with my bid, my limit order will be executed at 249.
     */
    private suspend fun placeLimitOrder(
        command: String,
        market: Market,
        price: Price,
        amount: Amount,
        tpe: BuyOrderType? = null,
        clientOrderId: Long? = null
    ): LimitOrderResult {
        val params = hashMap(
            "currencyPair" to market.toString(),
            "rate" to price.toString(),
            "amount" to amount.toString()
        )
        val additionalParams = run {
            var p = HashMap.empty<String, String>()
            if (tpe != null) p = p.put(tpe.id, "1")
            if (clientOrderId != null) p = p.put("clientOrderId", clientOrderId.toString())
            p
        }

        try {
            return callPrivateApi(command, jacksonTypeRef(), params.merge(additionalParams))
        } catch (e: Throwable) {
            throw handleBuySellErrors(e)
        }
    }

    private fun handleBuySellErrors(e: Throwable): Throwable {
        if (e.message == null) return e
        val msg = e.message!!

        var match: MatchResult? = OrdersCountExceededPattern.matchEntire(msg)

        if (match != null) {
            val (maxOrdersCount) = match.destructured
            return MaxOrdersExceededException(maxOrdersCount.toInt(), msg)
        }

        match = RateMustBeLessThanPattern.matchEntire(msg)

        if (match != null) {
            val (maxRate) = match.destructured
            return RateMustBeLessThanException(BigDecimal(maxRate), msg)
        }

        match = AmountMustBeAtLeastPattern.matchEntire(msg)

        if (match != null) {
            val (amount) = match.destructured
            return AmountMustBeAtLeastException(BigDecimal(amount), msg)
        }

        match = TotalMustBeAtLeastPattern.matchEntire(msg)

        if (match != null) {
            val (amount) = match.destructured
            return TotalMustBeAtLeastException(BigDecimal(amount), msg)
        }

        match = NotEnoughCryptoPattern.matchEntire(msg)

        if (match != null) {
            val (currency) = match.destructured
            return NotEnoughCryptoException(currency, msg)
        }

        match = OrderCompletedOrNotExistPattern.matchEntire(msg) ?: OrderWithClientIdCompletedOrNotExistPattern.matchEntire(msg)

        if (match != null) {
            val (orderIdStr) = match.destructured
            throw OrderCompletedOrNotExistException(orderIdStr.toLong(), msg)
        }

        if (UnableToFillOrderMsg == msg) {
            return UnableToFillOrderException
        }

        if (UnableToPlacePostOnlyOrderMsg == msg) {
            return UnableToPlacePostOnlyOrderException
        }

        if (InvalidOrderNumberMsg == msg) {
            return InvalidOrderNumberException
        }

        if (TransactionFailedMsg == msg) {
            return TransactionFailedException
        }

        if (AlreadyCalledMoveOrderMsg == msg) {
            return AlreadyCalledMoveOrderException
        }

        if (MarketDisabledMsg == msg) {
            return MarketDisabledException
        }

        if (OrderMatchingDisabledMsg == msg) {
            return OrderMatchingDisabledException
        }

        if (AlreadyCalledCancelOrMoveOrderMsg == msg) {
            return AlreadyCalledCancelOrMoveOrderException
        }

        if (InternalErrorMsg == msg) {
            return PoloniexInternalErrorException
        }

        if (MaintenanceModeMsg == msg) {
            return MaintenanceModeException
        }

        return e
    }

    open suspend fun cancelOrder(orderId: Long, orderIdType: CancelOrderIdType = CancelOrderIdType.Server): CancelOrder {
        val command = "cancelOrder"
        val orderIdTypeStr = when (orderIdType) {
            CancelOrderIdType.Server -> "orderNumber"
            CancelOrderIdType.Client -> "clientOrderId"
        }
        val params = hashMap(orderIdTypeStr to orderId.toString())
        try {
            val res = callPrivateApi(command, jacksonTypeRef<CancelOrderWrapper>(), params)
            if (!res.success) throw Exception(res.message)
            return CancelOrder(res.amount, res.fee.oneMinusAdjPoloniex, res.market, res.clientOrderId)
        } catch (e: Throwable) {
            if (e.message == null) throw e

            val msg = e.message!!

            val match = OrderCompletedOrNotExistPattern.matchEntire(msg) ?: OrderWithClientIdCompletedOrNotExistPattern.matchEntire(msg)

            if (match != null) {
                val (orderIdStr) = match.destructured
                throw OrderCompletedOrNotExistException(orderIdStr.toLong(), msg)
            }

            if (AlreadyCalledCancelOrMoveOrderMsg == msg) {
                throw AlreadyCalledCancelOrMoveOrderException
            }

            throw e
        }
    }

    // note: can be called 1 time per 2 minutes
    open suspend fun cancelAllOrders(market: Market?): CancelAllOrders {
        val command = "cancelAllOrders"
        val params = market?.let { hashMap("currencyPair" to it.toString()) } ?: HashMap.empty()
        val res = callPrivateApi(command, jacksonTypeRef<CancelAllOrdersWrapper>(), params)
        if (!res.success) throw Exception(res.message)
        return CancelAllOrders(res.orderNumbers)
    }

    /**
     * Cancels an order and places a new one of the same type in a single atomic transaction, meaning either both operations will succeed or both will fail.
     *
     * @param orderType "postOnly" or "immediateOrCancel" may be specified for exchange orders, but will have no effect on margin orders.
     * @return
     */
    open suspend fun moveOrder(
        orderId: Long,
        price: Price,
        amount: Amount? = null,
        orderType: BuyOrderType? = null,
        clientOrderId: Long? = null
    ): MoveOrderResult {
        val command = "moveOrder"
        val params = hashMap(
            some("orderNumber") to some(orderId.toString()),
            some("rate") to some(price.toString()),
            some("amount") to amount.option(),
            some("clientOrderId") to clientOrderId.option(),
            orderType.option().map { it.id } to some("1")
        )
            .toVavrStream()
            .filter { v -> v._1.isDefined && v._2.isDefined }
            .map { v -> tuple(v._1.get().toString(), v._2.get().toString()) }
            .toMap { it }

        try {
            return callPrivateApi(command, jacksonTypeRef<MoveOrderWrapper>(), params).run {
                val r = this
                if (r.success) {
                    MoveOrderResult(r.orderId!!, r.resultingTrades!!, r.fee.oneMinusAdjPoloniex, r.market, r.clientOrderId)
                } else {
                    throw Exception(r.errorMsg)
                }
            }
        } catch (e: Throwable) {
            throw handleBuySellErrors(e)
        }
    }

    /**
     * If you are enrolled in the maker-taker fee schedule, returns your current trading fees and trailing 30-day volume in BTC. This information is updated once every 24 hours.
     */
    open suspend fun feeInfo(): FeeInfo {
        val command = "returnFeeInfo"
        return callPrivateApi(command, jacksonTypeRef())
    }

    open suspend fun availableAccountBalances(): AvailableAccountBalance {
        val command = "returnAvailableAccountBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    open suspend fun marginTradableBalances(): Map<Market, Map<Currency, Amount>> {
        val command = "returnTradableBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    protected suspend fun <T : Any> callPublicApi(
        command: String,
        type: TypeReference<T>,
        queryParams: Map<String, String> = HashMap.empty(),
        limitRate: Boolean = true
    ): T {
        return handleKnownErrors(limitRate) {
            val qParams = hashMap("command" to command)
                .merge(queryParams)
                .iterator()
                .map { (k, v) -> "$k=$v" }.mkString("&")

            webClient.get()
                .uri("$PoloniexPrivatePublicHttpApiUrl/public?$qParams")
                .awaitExchange { bodyToJson(it, type) }
        }
    }

    protected suspend fun <T : Any> callPrivateApi(
        methodName: String,
        type: TypeReference<T>,
        postArgs: Map<String, String> = HashMap.empty(),
        limitRate: Boolean = true
    ): T {
        return handleKnownErrors(limitRate) {
            val postParamsPrivate = hashMap(
                "command" to methodName,
                "nonce" to System.currentTimeMillis().toString()
            )
            val postParams = postParamsPrivate.merge(postArgs)
            val body = postParams.iterator().map { (k, v) -> "$k=$v" }.mkString("&")
            val sign = signer.sign(body)

            webClient.post()
                .uri("$PoloniexPrivatePublicHttpApiUrl/tradingApi")
                .header("Key", poloniexApiKey)
                .header("Sign", sign)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(BodyInserters.fromPublisher(Mono.just(body), String::class.java))
                .awaitExchange { bodyToJson(it, type) }
        }
    }

    private suspend fun <T : Any> handleKnownErrors(
        limitRate: Boolean = true,
        block: suspend () -> T
    ): T {
        var data: T?

        while (true) {
            try {
                if (limitRate) delay(reqLimiter.get().toMillis())
                data = block()
                break
            } catch (e: IncorrectNonceException) {
                if (logger.isTraceEnabled) logger.trace(e.message)
                continue
            } catch (e: ApiCallLimitException) {
                logger.warn(e.message)
                continue
            } catch (e: Throwable) {
                throw e
            }
        }

        return data!!
    }

    private suspend fun <T : Any> bodyToJson(resp: ClientResponse, type: TypeReference<T>): T {
        val statusCode = resp.statusCode()
        val jsonString = resp.bodyToMono(String::class.java).awaitFirstOrNull()
            ?: throw Exception("Response body is empty")

        if (statusCode.is2xxSuccessful) {
            return try {
                @Suppress("BlockingMethodInNonBlockingContext")
                objectMapper.readValue(jsonString, type)
            } catch (respException: Exception) {
                val error: Error
                try {
                    error = objectMapper.readValue(jsonString)
                } catch (_: Exception) {
                    throw respException
                }

                if (error.msg != null) {
                    throw mapToKnownException(error.msg) ?: Exception(error.msg)
                } else {
                    throw respException
                }
            }
        } else {
            val error: Error
            try {
                error = objectMapper.readValue(jsonString)
            } catch (e: Throwable) {
                throw Exception("Error response not recognized: ${e.message}")
            }

            if (error.msg != null) {
                throw mapToKnownException(error.msg) ?: Exception(error.msg)
            } else {
                throw Exception("$statusCode: Can't call API")
            }
        }
    }

    private fun mapToKnownException(errorMsg: String): Throwable? {
        var match = ApiCallLimitPattern.matchEntire(errorMsg)

        if (match != null) {
            val (count) = match.destructured
            return ApiCallLimitException(count.toInt(), errorMsg)
        }

        match = IncorrectNonceMsgPattern.matchEntire(errorMsg)

        if (match != null) {
            val (provided, required) = match.destructured
            return IncorrectNonceException(provided.toLong(), required.toLong(), errorMsg)
        }

        return null
    }

    protected fun <T : Any> subscribeTo(
        channel: Int,
        respClass: TypeReference<T>,
        privateApi: Boolean = false
    ): Flow<T> = channelFlow {
        val main = this
        var messagesConsumptionJob: Job? = null
        val shouldUnsubscribe = AtomicBoolean(true)

        suspend fun startMessagesConsumption() = coroutineScope {
            channels[channel]!!.consumeEach { msg ->
                if (msg.data != null && msg.data != NullNode.instance) {
                    try {
                        val data = objectMapper.convertValue(msg.data, respClass)
                        main.send(data)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        logger.error("Can't parse websocket message: ${e.message}")
                    }
                } else if (msg.sequenceId == 1L && (msg.data == null || msg.data == NullNode.instance)) {
                    getChannelState(channel).value = true
                } else if (msg.sequenceId == 0L && (msg.data == null || msg.data == NullNode.instance)) {
                    throw SubscribeErrorException
                }
            }
        }

        suspend fun subscribeToChannel() = coroutineScope {
            while (true) {
                val payload = if (privateApi) {
                    val payload = "nonce=${System.currentTimeMillis()}"
                    val sign = signer.sign(payload)
                    val command = PrivateCommand(
                        CommandType.Subscribe,
                        channel,
                        poloniexApiKey,
                        payload,
                        sign
                    )
                    objectMapper.writeValueAsString(command)
                } else {
                    val command = Command(CommandType.Subscribe, channel)
                    objectMapper.writeValueAsString(command)
                }

                try {
                    withTimeout(15000) {
                        connectionOutput.send(payload)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    logger.warn("Can't send message over websocket channel: ${e.message}. Retry in 1 sec...")
                    delay(1000)
                    continue
                }

                if (logger.isDebugEnabled) logger.debug("Subscribed to $channel channel")
                break
            }
        }

        suspend fun unsubscribeFromChannel() {
            if (!shouldUnsubscribe.get()) {
                if (logger.isDebugEnabled) logger.debug("Connection has been already closed for channel $channel.")
                return
            }

            val command = Command(CommandType.Unsubscribe, channel)
            val json = objectMapper.writeValueAsString(command)

            try {
                withTimeout(2000) {
                    connectionOutput.send(json)
                }
                if (logger.isDebugEnabled) logger.debug("Unsubscribed from $channel channel")
            } catch (_: TimeoutCancellationException) {
                if (logger.isDebugEnabled) logger.debug("Can't unsubscribe from $channel channel. Connection closed ?")
            } catch (e: Throwable) {
                if (logger.isDebugEnabled) logger.debug("Can't unsubscribe from $channel channel: ${e.message}")
            }
        }

        fun CoroutineScope.subscribeAndStartMessagesConsumption() = this.launch {
            try {
                channels[channel] = BroadcastChannel(64)

                while (true) {
                    subscribeToChannel()

                    try {
                        try {
                            startMessagesConsumption()
                        } finally {
                            getChannelState(channel).value = false
                        }
                    } catch (e: SubscribeErrorException) {
                        logger.debug("Can't subscribe to channel $channel. Retrying...")
                        delay(100)
                    } catch (e: CancellationException) {
                        withContext(NonCancellable) {
                            unsubscribeFromChannel()
                        }
                        throw e
                    }
                }
            } finally {
                channels.remove(channel)
            }
        }

        coroutineScope {
            connection.collect { connected ->
                shouldUnsubscribe.set(connected)

                if (messagesConsumptionJob != null) {
                    messagesConsumptionJob!!.cancelAndJoin()
                    messagesConsumptionJob = null
                }

                if (connected) {
                    messagesConsumptionJob = subscribeAndStartMessagesConsumption()
                }
            }
        }
    }
}
