package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.util.HmacSha512Digest
import com.gitlab.dhorman.cryptotrader.util.RequestLimiter
import io.vavr.Tuple2
import io.vavr.collection.*
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.control.Option
import io.vavr.kotlin.*
import io.vertx.core.Vertx
import io.vertx.core.json.Json
import io.vertx.core.net.ProxyOptions
import io.vertx.core.net.ProxyType
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.reactivex.core.MultiMap
import io.vertx.reactivex.core.buffer.Buffer
import io.vertx.reactivex.core.http.WebSocket
import io.vertx.reactivex.ext.web.client.HttpResponse
import io.vertx.reactivex.ext.web.client.WebClient
import mu.KotlinLogging
import reactor.adapter.rxjava.toMono
import reactor.core.publisher.*
import java.math.BigDecimal
import java.time.Duration
import java.util.function.Function.identity
import io.vertx.reactivex.core.Vertx as VertxRx

private const val PoloniexPrivatePublicHttpApiUrl = "poloniex.com"
private const val PoloniexWebSocketApiUrl = "api2.poloniex.com"

/**
 * Documentation https://poloniex.com/support/api
 */
open class PoloniexApi(
    vertx: Vertx,
    private val poloniexApiKey: String,
    poloniexApiSecret: String
) {
    private val logger = KotlinLogging.logger {}
    private val signer = HmacSha512Digest(poloniexApiSecret)
    private val reqLimiter = RequestLimiter(allowedRequests = 6, perInterval = Duration.ofSeconds(1))

    private val vertxRx = VertxRx(vertx)

    private val httpOptions = run {
        val options = WebClientOptions()
            .setMaxWebsocketFrameSize(65536 * 4)
            .setMaxWebsocketMessageSize(65536 * 16)
            .setUserAgentEnabled(false)
            .setKeepAlive(false)
            .setSsl(true)

        if (System.getenv("HTTP_CERT_TRUST_ALL") != null) {
            options.isTrustAll = true
        }

        val httpProxy: Option<ProxyOptions> = Option.of(System.getenv("HTTP_PROXY_ENABLED")).map {
            val proxyOptions = ProxyOptions()
            val host = Option.of(System.getenv("HTTP_PROXY_HOST"))
            val port = Option.of(System.getenv("HTTP_PROXY_PORT")).flatMap { Try { Integer.parseInt(it) }.toOption() }
            val tpe = Option.of(System.getenv("HTTP_PROXY_TYPE")).map {
                when (it) {
                    "http" -> ProxyType.HTTP
                    "socks5" -> ProxyType.SOCKS5
                    else -> throw  Exception("Can't recognize HTTP_PROXY_TYPE option")
                }
            }

            if (host.isDefined) proxyOptions.host = host.get()
            if (port.isDefined) proxyOptions.port = port.get()
            if (tpe.isDefined) proxyOptions.type = tpe.get()

            proxyOptions
        }

        if (httpProxy.isDefined) {
            options.proxyOptions = httpProxy.get()
        }

        options
    }

    private val httpClient = vertxRx.createHttpClient(httpOptions)
    private val webclient = WebClient.create(vertxRx, httpOptions)

    private val websocket = run {
        Flux.create({ sink: FluxSink<WebSocket> ->
            Mono.from(
                httpClient.websocketStream(443, PoloniexWebSocketApiUrl, "/").toFlowable()
            )
                .single()
                .subscribe({ socket: WebSocket ->
                    logger.info("WebSocket connection established")

                    socket.closeHandler {
                        logger.info("WebSocket connection closed")
                        sink.complete()
                    }

                    socket.endHandler {
                        logger.info("WebSocket completed transmission")
                        sink.complete()
                    }

                    socket.exceptionHandler { err ->
                        logger.error("Exception occurred in WebSocket connection", err)
                        sink.error(err)
                    }

                    sink.onDispose {
                        socket.close()
                    }

                    sink.next(socket)
                }, { err ->
                    sink.error(err)
                })
        }, FluxSink.OverflowStrategy.LATEST)
            .replay(1)
            .refCount()
    }

    private val websocketMessages = run {
        val jsonReader = Json.mapper.readerFor(PushNotification::class.java)

        websocket
            .switchMap({ it.toFlowable().toFlux() }, Int.MAX_VALUE)
            .map { jsonReader.readValue<PushNotification>(it.bytes) }
            .share()
    }

    /**
     * Subscribe to ticker updates for all currency pairs.
     */
    open val tickerStream: Flux<Ticker> = run {
        Flux.create(create(DefaultChannel.TickerData.id, jacksonTypeRef<Ticker>()), FluxSink.OverflowStrategy.LATEST)
            .share()
    }

    open val dayExchangeVolumeStream: Flux<DayExchangeVolume> = run {
        Flux.create(
            create(DefaultChannel.DayExchangeVolume.id, jacksonTypeRef<DayExchangeVolume>()),
            FluxSink.OverflowStrategy.LATEST
        )
            .share()
    }

    open val accountNotificationStream: Flux<AccountNotification> = run {
        Flux.create(
            create(
                DefaultChannel.AccountNotifications.id,
                jacksonTypeRef<List<AccountNotification>>(),
                privateApi = true
            ), FluxSink.OverflowStrategy.BUFFER
        )
            .flatMapIterable(identity(), Int.MAX_VALUE)
            .share()
    }

    open fun orderBookStream(marketId: MarketId): Flux<Tuple2<PriceAggregatedBook, OrderBookNotification>> {
        val notifications: Flux<OrderBookNotification> =
            Flux.create(
                create(marketId, jacksonTypeRef<List<OrderBookNotification>>()),
                FluxSink.OverflowStrategy.BUFFER
            ).flatMapIterable(identity(), Int.MAX_VALUE)

        return notifications.scan(
            Tuple2<PriceAggregatedBook, OrderBookNotification>(
                PriceAggregatedBook(),
                null
            )
        ) { state, notification ->
            val (oldBook, _) = state

            val newBook = when (notification) {
                is OrderBookInit -> run {
                    val newAsks = oldBook.asks.merge(notification.asks)
                    val newBids = oldBook.bids.merge(notification.bids)
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
                        OrderType.Sell -> PriceAggregatedBook(modifyBook(oldBook.asks), oldBook.bids)
                        OrderType.Buy -> PriceAggregatedBook(oldBook.asks, modifyBook(oldBook.bids))
                    }
                }
                is OrderBookTrade -> oldBook
            }

            tuple(newBook, notification)
        }.skip(1).replay(1).refCount()
    }

    open fun orderBooksStream(marketIds: Traversable<MarketId>): Map<MarketId, Flux<Tuple2<PriceAggregatedBook, OrderBookNotification>>> {
        return marketIds.map { marketId -> tuple(marketId, orderBookStream(marketId)) }.toMap { it }
    }

    /**
     *
     * @return Returns the ticker for all markets.
     */
    open fun ticker(): Mono<Map<Market, Ticker0>> {
        val command = "returnTicker"
        return callPublicApi(command, jacksonTypeRef())
    }

    open fun tradeHistoryPublic(market: Market, fromDate: Long? = null, toDate: Long? = null): Mono<Array<TradeHistory>> {
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

    open fun currencies(): Mono<Map<Currency, CurrencyDetails>> {
        val command = "returnCurrencies"
        return callPublicApi(command, jacksonTypeRef())
    }

    /**
     * Returns all of your available balances
     */
    open fun availableBalances(): Mono<Map<Currency, BigDecimal>> {
        return callPrivateApi("returnBalances", jacksonTypeRef())
    }

    /**
     * Returns all of your balances, including available balance, balance on orders, and the estimated BTC value of your balance.
     */
    open fun completeBalances(): Mono<Map<Currency, CompleteBalance>> {
        return callPrivateApi("returnCompleteBalances", jacksonTypeRef())
    }


    /**
     * Returns your open orders for a given market, specified by the currencyPair.
     */
    open fun openOrders(market: Market): Mono<List<OpenOrder>> {
        return callPrivateApi(
            "returnOpenOrders",
            jacksonTypeRef(),
            hashMap("currencyPair" to market.toString())
        )
    }

    open fun allOpenOrders(): Mono<Map<Long, OpenOrderWithMarket>> {
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

    open fun tradeHistory(market: Market?): Mono<Map<Market, List<TradeHistoryPrivate>>> {
        val command = "returnTradeHistory"
        val params = hashMap("currencyPair" to (market?.toString() ?: "all"))
        return if (market == null) {
            callPrivateApi(command, jacksonTypeRef(), params)
        } else {
            callPrivateApi(command, jacksonTypeRef<List<TradeHistoryPrivate>>(), params).map { hashMap(market to it) }
        }
    }

    open fun orderTrades(orderNumber: BigDecimal): Mono<List<OrderTrade>> {
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
    open fun buy(market: Market, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?): Mono<BuySell> {
        return buySell("buy", market, price, amount, tpe)
    }

    open fun sell(market: Market, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?): Mono<BuySell> {
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

        return e
    }

    open fun cancelOrder(orderId: Long): Mono<CancelOrder> {
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
    open fun moveOrder(
        orderId: Long,
        price: Price,
        amount: Amount?,
        orderType: BuyOrderType?
    ): Mono<MoveOrderResult> {
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
    }

    /**
     * If you are enrolled in the maker-taker fee schedule, returns your current trading fees and trailing 30-day volume in BTC. This information is updated once every 24 hours.
     */
    open fun feeInfo(): Mono<FeeInfo> {
        val command = "returnFeeInfo"
        return callPrivateApi(command, jacksonTypeRef())
    }

    open fun availableAccountBalances(): Mono<AvailableAccountBalance> {
        val command = "returnAvailableAccountBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    open fun marginTradableBalances(): Mono<Map<Market, Map<Currency, Amount>>> {
        val command = "returnTradableBalances"
        return callPrivateApi(command, jacksonTypeRef())
    }

    private fun <T : Any> callPublicApi(
        command: String,
        type: TypeReference<T>,
        queryParams: Map<String, String> = HashMap.empty()
    ): Mono<T> {
        val qParams = hashMap("command" to command)
            .merge(queryParams)
            .iterator()
            .map { (k, v) -> "$k=$v" }.mkString("&")

        return webclient
            .get(443, PoloniexPrivatePublicHttpApiUrl, "/public?$qParams")
            .ssl(true)
            .rxSend()
            .toMono()
            .transform { handleApiCallResp(it, type) }
    }

    private fun <T : Any> callPrivateApi(
        methodName: String,
        type: TypeReference<T>,
        postArgs: Map<String, String> = HashMap.empty()
    ): Mono<T> {
        return Mono.defer {
            val postParamsPrivate = hashMap(
                "command" to methodName,
                "nonce" to System.currentTimeMillis().toString()
            )
            val postParams = postParamsPrivate.merge(postArgs)
            val sign = signer.sign(postParams.iterator().map { (k, v) -> "$k=$v" }.mkString("&"))

            val req = webclient
                .post(443, PoloniexPrivatePublicHttpApiUrl, "/tradingApi")
                .ssl(true)
                .putHeader("Key", poloniexApiKey)
                .putHeader("Sign", sign)

            val paramsx = MultiMap.caseInsensitiveMultiMap()

            with(paramsx) {
                for (p in postParams) add(p._1, p._2)
            }

            req.rxSendForm(paramsx).toMono()
        }.transform { handleApiCallResp(it, type) }
    }


    private fun <T : Any> handleApiCallResp(resp: Mono<HttpResponse<Buffer>>, type: TypeReference<T>): Mono<T> {
        return resp
            .map { bodyToJson(it, type) }
            .retryWhen { errors ->
                errors.concatMap<Int> { error ->
                    when (error) {
                        is IncorrectNonceException -> Flux.just(1)
                            .delaySubscription(
                                Mono.defer {
                                    Mono.delay(reqLimiter.get()).onErrorReturn(0)
                                }
                            )
                        is ApiCallLimitException -> run {
                            logger.warn(error.toString())
                            Flux.just(1)
                                .delaySubscription(
                                    Mono.defer {
                                        Mono.defer {
                                            Mono.delay(reqLimiter.get())
                                                .onErrorReturn(0)
                                        }.delaySubscription(Duration.ofSeconds(1))
                                    }
                                )
                        }
                        else -> Flux.error(error)
                    }
                }
            }
            .delaySubscription(Mono.defer { Mono.delay(reqLimiter.get()).onErrorReturn(0) })
    }

    private fun <T : Any> bodyToJson(resp: HttpResponse<Buffer>, type: TypeReference<T>): T {
        val statusCode = resp.statusCode()
        val respBytes = resp.body().bytes

        if (statusCode in 200..299) {
            return try {
                Json.mapper.readValue(respBytes, type)
            } catch (respException: Exception) {
                try {
                    val error = Json.mapper.readValue<Error>(respBytes)

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
                val error = Json.mapper.readValue<Error>(respBytes)

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

    private fun <T : Any> create(
        channel: Int,
        respClass: TypeReference<T>,
        privateApi: Boolean = false
    ): (FluxSink<T>) -> Unit = fun(sink: FluxSink<T>) {
        // Subscribe to ticker stream
        websocket.take(1).subscribe({ socket ->
            val subscribeCommandJson = if (privateApi) {
                val payload = "nonce=${System.currentTimeMillis()}"
                val sign = signer.sign(payload)
                val command = PrivateCommand(
                    CommandType.Subscribe,
                    channel,
                    poloniexApiKey,
                    payload,
                    sign
                )
                Json.mapper.writeValueAsString(command)
            } else {
                val command = Command(CommandType.Subscribe, channel)
                Json.mapper.writeValueAsString(command)
            }
            socket.writeTextMessage(subscribeCommandJson)
            logger.info("Subscribe to $channel channel")
        }, { err ->
            sink.error(err)
        })

        // Receive data
        val messagesSubscription = websocketMessages
            .filter { it.channel == channel && it.data != null && it.data != NullNode.instance }
            .map { Json.mapper.convertValue<T>(it.data!!, respClass) }
            .subscribe({ event ->
                sink.next(event)
            }, { err ->
                sink.error(err)
            }, {
                sink.complete()
            })

        // Unsubscribe from stream
        sink.onDispose {
            websocket.take(1).subscribe({ socket ->
                messagesSubscription.dispose()
                val command = Command(CommandType.Unsubscribe, channel)
                val json = Json.mapper.writeValueAsString(command)
                socket.writeTextMessage(json)
                logger.info("Unsubscribe from $channel channel")
            }, { err ->
                logger.trace {
                    logger.trace(err.message, err)
                }
            })
        }
    }
}
