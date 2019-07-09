package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.core.Market.Companion.toMarket
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import com.gitlab.dhorman.cryptotrader.util.HmacSha512Digest
import com.gitlab.dhorman.cryptotrader.util.RequestLimiter
import com.gitlab.dhorman.cryptotrader.util.share
import io.vavr.Tuple2
import io.vavr.collection.*
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.reactor.mono
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

private const val PoloniexPrivatePublicHttpApiUrl = "https://poloniex.com"
private const val PoloniexWebSocketApiUrl = "wss://api2.poloniex.com"

/**
 * Documentation https://docs.poloniex.com
 */
@Service
class PoloniexApi(
    @Qualifier("POLONIEX_API_KEY") private val poloniexApiKey: String,
    @Qualifier("POLONIEX_API_SECRET") private val poloniexApiSecret: String,
    private val webClient: WebClient,
    private val webSocketClient: WebSocketClient,
    private val objectMapper: ObjectMapper
) {
    private val logger = KotlinLogging.logger {}
    private val signer = HmacSha512Digest(poloniexApiSecret)
    private val reqLimiter = RequestLimiter(allowedRequests = 6, perInterval = Duration.ofSeconds(1))

    private val channels = ConcurrentHashMap<Int, BroadcastChannel<PushNotification>>()
    private val connectionOutput = Channel<String>(Channel.RENDEZVOUS)

    private val pushNotificationJsonReader = objectMapper.readerFor(PushNotification::class.java)

    val connection = run {
        channelFlow {
            while (true) {
                try {
                    val session = webSocketClient.execute(URI.create(PoloniexWebSocketApiUrl)) { session ->
                        FlowScope.mono {
                            send(true)
                            logger.info("Connection established with $PoloniexWebSocketApiUrl")

                            val receive = async {
                                session.receive().timeout(Duration.ofSeconds(3)).collect { msg ->
                                    val payloadBytes = ByteArray(msg.payload.readableByteCount())
                                    msg.payload.read(payloadBytes)

                                    if (logger.isTraceEnabled) {
                                        val payloadStr = String(payloadBytes, StandardCharsets.UTF_8)
                                        logger.trace("Received: $payloadStr")
                                    }

                                    try {
                                        val payload =
                                            pushNotificationJsonReader.readValue<PushNotification>(payloadBytes)

                                        channels[payload.channel]?.send(payload)
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: MismatchedInputException) {
                                        val error: Error
                                        try {
                                            error = objectMapper.readValue(payloadBytes)
                                        } catch (e: Exception) {
                                            throw e
                                        }

                                        if (error.msg != null) {
                                            if (error.msg == PermissionDeniedMsg) throw PermissionDeniedException
                                            if (error.msg == InvalidChannelMsg) throw InvalidChannelException
                                            throw e
                                        } else {
                                            throw e
                                        }
                                    } catch (e: Exception) {
                                        logger.error("Can't parse websocket message: ${e.message}")
                                    }
                                }
                            }

                            val send = async {
                                val output = connectionOutput.asFlux(coroutineContext).doOnNext {
                                    if (logger.isTraceEnabled) logger.trace("Sent: $it")
                                }.map(session::textMessage)

                                session.send(output).awaitFirstOrNull()
                            }

                            receive.await()
                            send.await()
                        }
                    }

                    session.awaitFirstOrNull()
                } catch (e: CancellationException) {
                    send(false)
                    logger.debug("Connection closed because internal job was cancelled")
                } catch (e: TimeoutException) {
                    send(false)
                    logger.debug("Haven't received any value from $PoloniexWebSocketApiUrl within specified interval")
                    delay(1000)
                } catch (e: InvalidChannelException) {
                    send(false)
                    logger.error(e.message)
                    delay(1000)
                } catch (e: PermissionDeniedException) {
                    send(false)
                    logger.debug("WebSocket private channel sent permission denied message")
                } catch (e: Throwable) {
                    send(false)
                    logger.error(e.message)
                    delay(1000)
                }
            }
        }.distinctUntilChanged().share(1)
    }

    /**
     * Subscribe to ticker updates for all currency pairs.
     */
    val tickerStream: Flow<Ticker> = run {
        subscribeTo(DefaultChannel.TickerData.id, jacksonTypeRef<Ticker>()).conflate().share()
    }

    val dayExchangeVolumeStream: Flow<DayExchangeVolume> = run {
        subscribeTo(DefaultChannel.DayExchangeVolume.id, jacksonTypeRef<DayExchangeVolume>())
            .share()
    }

    val accountNotificationStream: Flow<List<AccountNotification>> = run {
        subscribeTo(
            DefaultChannel.AccountNotifications.id,
            jacksonTypeRef<List<AccountNotification>>(),
            privateApi = true
        )
            .share()
    }

    fun orderBookStream(marketId: MarketId): Flow<Tuple2<PriceAggregatedBook, OrderBookNotification>> {
        return channelFlow {
            while (true) {
                try {
                    coroutineScope {
                        var book = PriceAggregatedBook()

                        launch {
                            connection.collect { connected ->
                                if (!connected) throw Exception("Connection closed")
                            }
                        }

                        subscribeTo(marketId, jacksonTypeRef<List<OrderBookNotification>>())
                            .collect { notificationList ->
                                for (notification in notificationList) {
                                    book = when (notification) {
                                        is OrderBookInit -> run {
                                            val newAsks = book.asks.merge(notification.asks)
                                            val newBids = book.bids.merge(notification.bids)
                                            PriceAggregatedBook(newAsks, newBids)
                                        }
                                        is OrderBookUpdate -> run {
                                            fun modifyBook(book: TreeMap<Price, Amount>): TreeMap<Price, Amount> =
                                                if (notification.amount.compareTo(BigDecimal.ZERO) == 0) {
                                                    book.remove(notification.price)
                                                } else {
                                                    book.put(notification.price, notification.amount)
                                                }

                                            when (notification.orderType) {
                                                OrderType.Sell -> PriceAggregatedBook(modifyBook(book.asks), book.bids)
                                                OrderType.Buy -> PriceAggregatedBook(book.asks, modifyBook(book.bids))
                                            }
                                        }
                                        is OrderBookTrade -> book
                                    }

                                    send(tuple(book, notification))
                                }
                            }
                    }
                } catch (e: CancellationException) {
                    delay(500)
                } catch (e: Exception) {
                    delay(1000)
                }
            }
        }.share(1)
    }

    fun orderBooksStream(marketIds: Traversable<MarketId>): Map<MarketId, Flow<Tuple2<PriceAggregatedBook, OrderBookNotification>>> {
        return marketIds.map { marketId -> tuple(marketId, orderBookStream(marketId)) }.toMap { it }
    }

    suspend fun dayVolume(): Map<Market, Tuple2<Amount, Amount>> {
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

    /**
     *
     * @return Returns the ticker for all markets.
     */
    suspend fun ticker(): Map<Market, Ticker0> {
        val command = "returnTicker"
        return callPublicApi(command, jacksonTypeRef())
    }

    suspend fun tradeHistoryPublic(
        market: Market,
        fromTs: Instant? = null,
        toTs: Instant? = null
    ): Array<TradeHistory> {
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

    suspend fun currencies(): Map<Currency, CurrencyDetails> {
        val command = "returnCurrencies"
        return callPublicApi(command, jacksonTypeRef())
    }

    /**
     * Returns all of your available balances
     */
    suspend fun availableBalances(): Map<Currency, BigDecimal> {
        return callPrivateApi("returnBalances", jacksonTypeRef())
    }

    /**
     * Returns all of your balances, including available balance, balance on orders, and the estimated BTC value of your balance.
     */
    suspend fun completeBalances(): Map<Currency, CompleteBalance> {
        return callPrivateApi("returnCompleteBalances", jacksonTypeRef())
    }

    /**
     * Returns your open orders for a given market, specified by the currencyPair.
     */
    suspend fun openOrders(market: Market): List<OpenOrder> {
        return callPrivateApi(
            "returnOpenOrders",
            jacksonTypeRef(),
            hashMap("currencyPair" to market.toString())
        )
    }

    suspend fun allOpenOrders(): Map<Long, OpenOrderWithMarket> {
        val command = "returnOpenOrders"
        val params = hashMap("currencyPair" to "all")
        return callPrivateApi(command, jacksonTypeRef<Map<Market, List<OpenOrder>>>(), params)
            .flatMap<OpenOrderWithMarket> { marketOrders ->
                marketOrders._2.map { order -> OpenOrderWithMarket.from(order, marketOrders._1) }
            }
            .toMap({ it.orderId }, { it })
    }

    suspend fun orderStatus(orderId: Long): OrderStatus? {
        val command = "returnOrderStatus"
        val params = hashMap("orderNumber" to orderId.toString())
        val json = callPrivateApi(command, jacksonTypeRef<JsonNode>(), params)

        try {
            val res = objectMapper.convertValue<OrderStatusWrapper>(json, jacksonTypeRef<OrderStatusWrapper>())
            return if (res.success) {
                res.result.getOrNull(orderId)
            } else {
                null
            }
        } catch (e: Exception) {
            try {
                val res =
                    objectMapper.convertValue<OrderStatusErrorWrapper>(json, jacksonTypeRef<OrderStatusErrorWrapper>())
                val errorMsg = res.result.getOrNull("error")
                if (logger.isTraceEnabled && errorMsg != null) logger.trace("Can't get order status: $errorMsg")
                return null
            } catch (e: Exception) {
                throw e
            }
        }
    }

    suspend fun tradeHistory(
        market: Market? = null,
        fromTs: Instant? = null,
        toTs: Instant? = null,
        limit: Long? = null
    ): Map<Market, List<TradeHistoryPrivate>> {
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

    suspend fun orderTrades(orderNumber: Long): List<OrderTrade> {
        val command = "returnOrderTrades"
        val params = hashMap("orderNumber" to orderNumber.toString())
        return try {
            callPrivateApi(command, jacksonTypeRef(), params)
        } catch (e: Exception) {
            if (e.message == OrderNotFoundPattern) {
                List.empty()
            } else {
                throw e
            }
        }
    }

    /**
     * Places a limit buy order in a given market.
     *
     * @param tpe You may optionally set "fillOrKill", "immediateOrCancel", "postOnly".
     *            A fill-or-kill order will either fill in its entirety or be completely aborted.
     *            An immediate-or-cancel order can be partially or completely filled, but any portion of the order that cannot be filled immediately will be canceled rather than left on the order book.
     *            A post-only order will only be placed if no portion of it fills immediately; this guarantees you will never pay the taker fee on any part of the order that fills.
     * @return If successful, the method will return the order number.
     */
    suspend fun buy(market: Market, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?): BuySell {
        return buySell("buy", market, price, amount, tpe)
    }

    suspend fun sell(market: Market, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?): BuySell {
        return buySell("sell", market, price, amount, tpe)
    }

    /**
     * A limit order is one of the most basic order types. It allows the trader to specify a price and amount they would like to buy or sell.
     *
     * Example: If the current market price is 250 and I want to buy lower than that at 249, then I would place a limit buy order at 249. If the market reaches 249 and a seller’s ask matches with my bid, my limit order will be executed at 249.
     */
    private suspend fun buySell(
        command: String,
        market: Market,
        price: Price,
        amount: Amount,
        tpe: BuyOrderType? = null
    ): BuySell {
        val params = hashMap(
            "currencyPair" to market.toString(),
            "rate" to price.toString(),
            "amount" to amount.toString()
        )
        val additionalParams = tpe?.let { hashMap(it.id to "1") } ?: HashMap.empty()

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

        match = OrderCompletedOrNotExistPattern.matchEntire(msg)

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

        return e
    }

    suspend fun cancelOrder(orderId: Long): CancelOrder {
        val command = "cancelOrder"
        val params = hashMap("orderNumber" to orderId.toString())
        try {
            val res = callPrivateApi(command, jacksonTypeRef<CancelOrderWrapper>(), params)
            if (!res.success) throw Exception(res.message)
            return CancelOrder(res.amount, res.fee, res.market)
        } catch (e: Exception) {
            if (e.message == null) throw e

            val msg = e.message!!

            val match: MatchResult? = OrderCompletedOrNotExistPattern.matchEntire(msg)

            if (match != null) {
                val (orderIdStr) = match.destructured
                throw OrderCompletedOrNotExistException(orderIdStr.toLong(), msg)
            }

            throw e
        }
    }

    /**
     * Cancels an order and places a new one of the same type in a single atomic transaction, meaning either both operations will succeed or both will fail.
     *
     * @param orderType "postOnly" or "immediateOrCancel" may be specified for exchange orders, but will have no effect on margin orders.
     * @return
     */
    suspend fun moveOrder(
        orderId: Long,
        price: Price,
        amount: Amount? = null,
        orderType: BuyOrderType? = null
    ): MoveOrderResult {
        val command = "moveOrder"
        val params = hashMap(
            "orderNumber" to orderId.toString(),
            "rate" to price.toString()
        )
        val optParams = hashMap(
            some("amount") to amount.option(),
            orderType.option().map { it.id } to some("1")
        )
            .iterator()
            .filter { v -> v._1.isDefined && v._2.isDefined }
            .map { v -> tuple(v._1.get().toString(), v._2.get().toString()) }
            .toMap { it }

        try {
            return callPrivateApi(
                command,
                jacksonTypeRef<MoveOrderWrapper>(),
                params.merge(optParams)
            ).run {
                val r = this
                if (r.success) {
                    MoveOrderResult(r.orderId!!, r.resultingTrades!!, r.fee, r.market)
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
    suspend fun feeInfo(): FeeInfo {
        val command = "returnFeeInfo"
        return callPrivateApi(command, jacksonTypeRef())
    }

    suspend fun availableAccountBalances(): AvailableAccountBalance {
        val command = "returnAvailableAccountBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    suspend fun marginTradableBalances(): Map<Market, Map<Currency, Amount>> {
        val command = "returnTradableBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    private suspend fun <T : Any> callPublicApi(
        command: String,
        type: TypeReference<T>,
        queryParams: Map<String, String> = HashMap.empty(),
        limitRate: Boolean = true
    ): T {
        return convertBodyToJsonAndHandleKnownErrors(type, limitRate) {
            val qParams = hashMap("command" to command)
                .merge(queryParams)
                .iterator()
                .map { (k, v) -> "$k=$v" }.mkString("&")

            webClient.get()
                .uri("$PoloniexPrivatePublicHttpApiUrl/public?$qParams")
                .exchange()
                .awaitSingle()
        }
    }

    private suspend fun <T : Any> callPrivateApi(
        methodName: String,
        type: TypeReference<T>,
        postArgs: Map<String, String> = HashMap.empty(),
        limitRate: Boolean = true
    ): T {
        return convertBodyToJsonAndHandleKnownErrors(type, limitRate) {
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
                .exchange()
                .awaitSingle()
        }
    }

    private suspend fun <T : Any> convertBodyToJsonAndHandleKnownErrors(
        type: TypeReference<T>,
        limitRate: Boolean = true,
        block: suspend () -> ClientResponse
    ): T {
        var data: T?

        while (true) {
            try {
                if (limitRate) delay(reqLimiter.get().toMillis())
                val resp = block()
                data = bodyToJson(resp, type)
                break
            } catch (e: IncorrectNonceException) {
                if (logger.isTraceEnabled) logger.trace(e.message)
                continue
            } catch (e: ApiCallLimitException) {
                logger.warn(e.message)
                continue
            } catch (e: Exception) {
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
            } catch (e: Exception) {
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

    private fun <T : Any> subscribeTo(
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
                        val data = objectMapper.convertValue<T>(msg.data, respClass)
                        main.send(data)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        logger.error("Can't parse websocket message: ${e.message}")
                    }
                    // TODO: Investigate websocket error responses in details
                }/* else if (msg.sequenceId == 0L) {
                    throw SubscribeErrorException
                }*/
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
                    connectionOutput.send(payload)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.warn("Can't send message over websocket channel: ${e.message}. Retry in 1 sec...")
                    delay(1000)
                    continue
                }

                if (logger.isDebugEnabled) logger.debug("Subscribe to $channel channel")
                break
            }
        }

        suspend fun unsubscribeFromChannel() {
            if (!shouldUnsubscribe.get()) return

            val command = Command(CommandType.Unsubscribe, channel)
            val json = objectMapper.writeValueAsString(command)

            try {
                withTimeout(2000) {
                    connectionOutput.send(json)
                }
                if (logger.isDebugEnabled) logger.debug("Unsubscribe from $channel channel")
            } catch (_: TimeoutCancellationException) {
                if (logger.isDebugEnabled) logger.debug("Can't unsubscribe from $channel channel. Connection closed ?")
            } catch (e: Exception) {
                if (logger.isDebugEnabled) logger.debug("Can't unsubscribe from $channel channel: ${e.message}")
            }
        }

        fun CoroutineScope.subscribeAndStartMessagesConsumption() = this.launch {
            try {
                channels[channel] = BroadcastChannel(64)

                while (true) {
                    subscribeToChannel()

                    try {
                        startMessagesConsumption()
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
