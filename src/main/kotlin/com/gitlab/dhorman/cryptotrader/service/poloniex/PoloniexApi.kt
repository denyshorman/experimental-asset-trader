package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import com.gitlab.dhorman.cryptotrader.util.HmacSha512Digest
import com.gitlab.dhorman.cryptotrader.util.RequestLimiter
import io.vavr.Tuple2
import io.vavr.collection.*
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.reactor.flux
import kotlinx.coroutines.reactor.mono
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.core.publisher.EmitterProcessor
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.net.URI
import java.time.Duration
import java.util.function.Function.identity

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

    private val connectionInput = EmitterProcessor.create<PushNotification>()
    private val connectionOutput = Channel<String>()

    val connection = run {
        FlowScope.flux {
            val jsonReader = objectMapper.readerFor(PushNotification::class.java)

            while (isActive) {
                try {
                    webSocketClient.execute(URI.create(PoloniexWebSocketApiUrl)) { session ->
                        FlowScope.mono {
                            send(true)
                            logger.info("Connection established with $PoloniexWebSocketApiUrl")

                            val receive = async {
                                session.receive().timeout(Duration.ofSeconds(2)).collect { msg ->
                                    try {
                                        val payload =
                                            jsonReader.readValue<PushNotification>(msg.payload.asInputStream())
                                        connectionInput.onNext(payload)
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Exception) {
                                        logger.error("Can't parse websocket message", e)
                                    }
                                }
                            }

                            val send = async {
                                val output = connectionOutput.asFlux().map(session::textMessage)
                                session.send(output).awaitFirstOrNull()
                            }

                            receive.await()
                            send.await()
                        }
                    }.awaitFirstOrNull()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    send(false)
                    if (logger.isDebugEnabled) logger.warn(e.message, e)
                    delay(1000)
                    continue
                }
            }
        }.distinctUntilChanged().replay(1).refCount()
    }

    /**
     * Subscribe to ticker updates for all currency pairs.
     */
    val tickerStream: Flux<Ticker> = run {
        subscribeTo(DefaultChannel.TickerData.id, jacksonTypeRef<Ticker>()).share()
    }

    val dayExchangeVolumeStream: Flux<DayExchangeVolume> = run {
        subscribeTo(DefaultChannel.DayExchangeVolume.id, jacksonTypeRef<DayExchangeVolume>()).share()
    }

    val accountNotificationStream: Flux<AccountNotification> = run {
        subscribeTo(
            DefaultChannel.AccountNotifications.id,
            jacksonTypeRef<List<AccountNotification>>(),
            privateApi = true
        )
            .flatMapIterable(identity(), Int.MAX_VALUE)
            .share()
    }

    // TODO: Send notification list because they send atomic operation as list
    fun orderBookStream(marketId: MarketId): Flux<Tuple2<PriceAggregatedBook, OrderBookNotification>> {
        return FlowScope.flux {
            while (isActive) {
                try {
                    coroutineScope {
                        var book = PriceAggregatedBook()

                        launch {
                            connection.collect { connected ->
                                if (!connected) throw Exception("Connection closed")
                            }
                        }

                        subscribeTo(marketId, jacksonTypeRef<List<OrderBookNotification>>()).onBackpressureBuffer()
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
                    throw e
                } catch (e: Exception) {
                    delay(1000)
                    continue
                }
            }
        }.replay(1).refCount()
    }

    fun orderBooksStream(marketIds: Traversable<MarketId>): Map<MarketId, Flux<Tuple2<PriceAggregatedBook, OrderBookNotification>>> {
        return marketIds.map { marketId -> tuple(marketId, orderBookStream(marketId)) }.toMap { it }
    }

    /**
     *
     * @return Returns the ticker for all markets.
     */
    fun ticker(): Mono<Map<Market, Ticker0>> {
        val command = "returnTicker"
        return callPublicApi(command, jacksonTypeRef())
    }

    fun tradeHistoryPublic(market: Market, fromDate: Long? = null, toDate: Long? = null): Mono<Array<TradeHistory>> {
        val command = "returnTradeHistory"

        val params = hashMap(
            "currencyPair" to some(market),
            "start" to fromDate.option(),
            "end" to toDate.option()
        )
            .iterator()
            .filter { it._2.isDefined }
            .map { v -> tuple(v._1, v._2.get().toString()) }
            .toMap { it }

        return callPublicApi(command, jacksonTypeRef(), params)
    }

    fun currencies(): Mono<Map<Currency, CurrencyDetails>> {
        val command = "returnCurrencies"
        return callPublicApi(command, jacksonTypeRef())
    }

    /**
     * Returns all of your available balances
     */
    fun availableBalances(): Mono<Map<Currency, BigDecimal>> {
        return callPrivateApi("returnBalances", jacksonTypeRef())
    }

    /**
     * Returns all of your balances, including available balance, balance on orders, and the estimated BTC value of your balance.
     */
    fun completeBalances(): Mono<Map<Currency, CompleteBalance>> {
        return callPrivateApi("returnCompleteBalances", jacksonTypeRef())
    }


    /**
     * Returns your open orders for a given market, specified by the currencyPair.
     */
    fun openOrders(market: Market): Mono<List<OpenOrder>> {
        return callPrivateApi(
            "returnOpenOrders",
            jacksonTypeRef(),
            hashMap("currencyPair" to market.toString())
        )
    }

    fun allOpenOrders(): Mono<Map<Long, OpenOrderWithMarket>> {
        val command = "returnOpenOrders"
        val params = hashMap("currencyPair" to "all")
        return callPrivateApi(command, jacksonTypeRef<Map<Market, List<OpenOrder>>>(), params)
            .map { openOrdersMap ->
                openOrdersMap
                    .flatMap<OpenOrderWithMarket> { marketOrders ->
                        marketOrders._2.map { order -> OpenOrderWithMarket.from(order, marketOrders._1) }
                    }
                    .toMap({ it.orderId }, { it })
            }
    }

    fun orderStatus(orderId: Long): Mono<OrderStatus> {
        TODO("Implement orderStatus")
    }

    fun tradeHistory(market: Market?): Mono<Map<Market, List<TradeHistoryPrivate>>> {
        val command = "returnTradeHistory"
        val params = hashMap("currencyPair" to (market?.toString() ?: "all"))
        return if (market == null) {
            callPrivateApi(command, jacksonTypeRef(), params)
        } else {
            callPrivateApi(command, jacksonTypeRef<List<TradeHistoryPrivate>>(), params).map { hashMap(market to it) }
        }
    }

    fun orderTrades(orderNumber: BigDecimal): Mono<List<OrderTrade>> {
        val command = "returnOrderTrades"
        val params = hashMap("orderNumber" to orderNumber.toString())
        return callPrivateApi(command, jacksonTypeRef(), params)
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
    fun buy(market: Market, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?): Mono<BuySell> {
        return buySell("buy", market, price, amount, tpe)
    }

    fun sell(market: Market, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?): Mono<BuySell> {
        return buySell("sell", market, price, amount, tpe)
    }

    /**
     * A limit order is one of the most basic order types. It allows the trader to specify a price and amount they would like to buy or sell.
     *
     * Example: If the current market price is 250 and I want to buy lower than that at 249, then I would place a limit buy order at 249. If the market reaches 249 and a seller’s ask matches with my bid, my limit order will be executed at 249.
     */
    private fun buySell(
        command: String,
        market: Market,
        price: Price,
        amount: Amount,
        tpe: BuyOrderType?
    ): Mono<BuySell> {
        val params = hashMap(
            "currencyPair" to market.toString(),
            "rate" to price.toString(),
            "amount" to amount.toString()
        )
        val additionalParams = tpe?.let { hashMap(it.toString() to "1") } ?: HashMap.empty()

        return callPrivateApi(command, jacksonTypeRef<BuySell>(), params.merge(additionalParams))
            .onErrorMap { handleBuySellErrors(it) }
    }

    private fun handleBuySellErrors(e: Throwable): Throwable {
        if (e.message == null) return e
        val msg = e.message!!

        var match: MatchResult? = OrdersCountExceededPattern.matchEntire(msg)

        if (match != null) {
            val (maxOrdersCount) = match.destructured
            return MaxOrdersExcidedException(maxOrdersCount.toInt(), msg)
        }

        match = RateMustBeLessThanPattern.matchEntire(msg)

        if (match != null) {
            val (maxRate) = match.destructured
            return RateMustBeLessThanException(BigDecimal(maxRate), msg)
        }

        match = TotalMustBeAtLeastPattern.matchEntire(msg)

        if (match != null) {
            val (amount) = match.destructured
            return TotalMustBeAtLeastException(BigDecimal(amount), msg)
        }

        if (InvalidOrderNumberPattern == msg) {
            return InvalidOrderNumberException
        }

        if (TransactionFailedPattern == msg) {
            return TransactionFailedException
        }

        return e
    }

    fun cancelOrder(orderId: Long): Mono<CancelOrder> {
        val command = "cancelOrder"
        val params = hashMap("orderNumber" to orderId.toString())
        return callPrivateApi(command, jacksonTypeRef(), params)
    }

    /**
     * Cancels an order and places a new one of the same type in a single atomic transaction, meaning either both operations will succeed or both will fail.
     *
     * @param orderType "postOnly" or "immediateOrCancel" may be specified for exchange orders, but will have no effect on margin orders.
     * @return
     */
    fun moveOrder(
        orderId: Long,
        price: Price,
        amount: Amount?,
        orderType: BuyOrderType?
    ): Mono<MoveOrderResult2> {
        val command = "moveOrder"
        val params = hashMap(
            "orderNumber" to orderId.toString(),
            "rate" to price.toString()
        )
        val optParams = hashMap(
            some("amount") to amount.option(),
            orderType.option() to some("1")
        )
            .iterator()
            .filter { v -> v._1.isDefined && v._2.isDefined }
            .map { v -> tuple(v._1.get().toString(), v._2.get().toString()) }
            .toMap { it }

        return callPrivateApi(command, jacksonTypeRef<MoveOrderResult>(), params.merge(optParams))
            .onErrorMap { handleBuySellErrors(it) }
            .map {
                if (it.success) {
                    MoveOrderResult2(
                        it.orderId!!,
                        it.resultingTrades!!,
                        it.fee,
                        it.currencyPair
                    )
                } else {
                    throw handleBuySellErrors(Exception(it.errorMsg))
                }
            }
    }

    /**
     * If you are enrolled in the maker-taker fee schedule, returns your current trading fees and trailing 30-day volume in BTC. This information is updated once every 24 hours.
     */
    fun feeInfo(): Mono<FeeInfo> {
        val command = "returnFeeInfo"
        return callPrivateApi(command, jacksonTypeRef())
    }

    fun availableAccountBalances(): Mono<AvailableAccountBalance> {
        val command = "returnAvailableAccountBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    fun marginTradableBalances(): Mono<Map<Market, Map<Currency, Amount>>> {
        val command = "returnTradableBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    private fun <T : Any> callPublicApi(
        command: String,
        type: TypeReference<T>,
        queryParams: Map<String, String> = HashMap.empty()
    ): Mono<T> = FlowScope.mono {
        convertBodyToJsonAndHandleKnownErrors(type) {
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

    private fun <T : Any> callPrivateApi(
        methodName: String,
        type: TypeReference<T>,
        postArgs: Map<String, String> = HashMap.empty()
    ): Mono<T> = FlowScope.mono {
        convertBodyToJsonAndHandleKnownErrors(type) {
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
        block: suspend () -> ClientResponse
    ): T {
        var data: T?

        while (true) {
            try {
                delay(reqLimiter.get().toMillis()) // TODO: Investigate delay for public and private API calls
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
                objectMapper.readValue(jsonString, type)
            } catch (respException: Exception) {
                try {
                    val error = objectMapper.readValue<Error>(jsonString)

                    if (error.msg != null) {
                        handleKnownError(error.msg)
                    } else {
                        throw Exception("Received http error with empty description", respException)
                    }
                } catch (_: Exception) {
                    throw respException
                }
            }
        } else {
            try {
                val error = objectMapper.readValue<Error>(jsonString)

                if (error.msg != null) {
                    handleKnownError(error.msg)
                } else {
                    throw Exception("Received http error with empty description")
                }
            } catch (e: Exception) {
                throw Exception("Error response not recognized", e)
            }
        }
    }

    private fun handleKnownError(errorMsg: String): Nothing {
        var match = ApiCallLimitPattern.matchEntire(errorMsg)

        if (match != null) {
            val (count) = match.destructured
            throw ApiCallLimitException(count.toInt(), errorMsg)
        }

        match = IncorrectNonceMsgPattern.matchEntire(errorMsg)

        if (match != null) {
            val (provided, required) = match.destructured
            throw IncorrectNonceException(provided.toLong(), required.toLong(), errorMsg)
        }

        throw Exception(errorMsg)
    }

    private fun <T : Any> subscribeTo(
        channel: Int,
        respClass: TypeReference<T>,
        privateApi: Boolean = false
    ): Flux<T> = FlowScope.flux {
        val main = this
        var messagesConsumptionJob: Job? = null

        fun startMessagesConsumption() = launch {
            connectionInput.onBackpressureBuffer().collect { msg ->
                if (msg.channel == channel && msg.data != null && msg.data != NullNode.instance) {
                    try {
                        val data = objectMapper.convertValue<T>(msg.data, respClass)
                        main.send(data)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        logger.error("Can't parse websocket message", e)
                    }
                }
            }
        }

        fun subscribeToChannel() = launch {
            while (isActive) {
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

        fun subscribeAndStartMessagesConsumption() = launch {
            subscribeToChannel().join()

            try {
                startMessagesConsumption().join()
            } catch (e: CancellationException) {
                withContext(NonCancellable) {
                    unsubscribeFromChannel()
                }
                throw e
            }
        }

        connection.collect { connected ->
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
