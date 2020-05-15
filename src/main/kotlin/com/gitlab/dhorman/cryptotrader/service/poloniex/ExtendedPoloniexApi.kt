package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.DisconnectedException
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.InvalidDepthException
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.InvalidMarketException
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.util.share
import io.netty.handler.timeout.ReadTimeoutException
import io.netty.handler.timeout.WriteTimeoutException
import io.vavr.Tuple2
import io.vavr.collection.HashMap
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.collection.TreeMap
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import java.io.IOException
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.ConnectException
import java.net.SocketException
import java.net.UnknownHostException
import java.time.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

typealias MarketIntMap = Map<MarketId, Market>
typealias MarketStringMap = Map<Market, MarketId>
typealias MarketData = Tuple2<MarketIntMap, MarketStringMap>

data class OrderBookData(
    val market: Market,
    val marketId: MarketId,
    val book: PriceAggregatedBook,
    val notification: List<OrderBookNotification>
)
typealias OrderBookDataMap = Map<MarketId, Flow<OrderBookData>>

@Service
class ExtendedPoloniexApi(
    @Qualifier("POLONIEX_API_KEY") poloniexApiKey: String,
    @Qualifier("POLONIEX_API_SECRET") poloniexApiSecret: String,
    webClient: WebClient,
    webSocketClient: WebSocketClient,
    objectMapper: ObjectMapper,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator,
    private val clock: Clock
) : PoloniexApi(poloniexApiKey, poloniexApiSecret, webClient, webSocketClient, objectMapper) {
    private val logger = KotlinLogging.logger {}
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val streamSynchronizer = StreamSynchronizer()
    private val marketOrderBook = ConcurrentHashMap<MarketId, Flow<OrderBookData>>()

    private val orderIdGenerator: AtomicLong by lazy(LazyThreadSafetyMode.PUBLICATION) {
        AtomicLong(Instant.now(clock).toEpochMilli())
    }

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            runBlocking {
                scope.coroutineContext[Job]?.cancelAndJoin()
            }
        })
    }

    val currencyStream: Flow<Tuple2<Map<Currency, CurrencyDetails>, Map<Int, Currency>>> = run {
        channelFlow {
            while (isActive) {
                try {
                    val currencies = super.currencies()
                    send(tuple(currencies, currencies.map { k, v -> tuple(v.id, k) }))
                    delay(10 * 60 * 1000)
                } catch (e: CancellationException) {
                } catch (e: Throwable) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch currencies from Poloniex because ${e.message}")
                    delay(2000)
                }
            }
        }.share(1, Duration.ofMinutes(30), scope)
    }

    val marketStream: Flow<MarketData> = run {
        channelFlow {
            var prevMarketsSet = mutableSetOf<Int>()

            while (isActive) {
                try {
                    val tickers = super.ticker()
                    var marketIntStringMap: Map<MarketId, Market> = HashMap.empty()
                    var marketStringIntMap: Map<Market, MarketId> = HashMap.empty()
                    val currentMarketsSet = mutableSetOf<Int>()

                    for ((market, tick) in tickers) {
                        if (!tick.isFrozen) {
                            marketIntStringMap = marketIntStringMap.put(tick.id, market)
                            marketStringIntMap = marketStringIntMap.put(market, tick.id)
                            currentMarketsSet.add(tick.id)
                        }
                    }

                    if (!prevMarketsSet.containsAll(currentMarketsSet)) {
                        prevMarketsSet = currentMarketsSet
                        send(tuple(marketIntStringMap, marketStringIntMap))
                    }

                    delay(10 * 60 * 1000)
                } catch (e: CancellationException) {
                } catch (e: Throwable) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch markets from Poloniex: ${e.message}")
                    delay(2000)
                }
            }
        }.share(1, Duration.ofMinutes(30), scope)
    }

    val balanceStream: Flow<Map<Currency, Tuple2<Amount, Amount>>> = run {
        channelFlow {
            try {
                while (isActive) {
                    try {
                        coroutineScope {
                            streamSynchronizer.lockStream(LockStreamKeys.BalanceStreamKey)

                            val rawApiBalancesFuture = async { super.completeBalances() }
                            val allOpenOrdersFuture = async { super.allOpenOrders() }

                            val rawApiBalances = rawApiBalancesFuture.await()
                            var allOpenOrders = allOpenOrdersFuture.await()

                            val balanceOnOrders = allOpenOrders.groupBy({ (_, order) ->
                                if (order.type == OrderType.Buy) {
                                    order.market.baseCurrency
                                } else {
                                    order.market.quoteCurrency
                                }
                            }, { (_, order) ->
                                if (order.type == OrderType.Buy) {
                                    amountCalculator.fromAmountBuy(order.amount, order.price, BigDecimal.ONE)
                                } else {
                                    order.amount
                                }
                            }).mapValues { it.value.reduce { a, b -> a + b } }

                            var availableAndOnOrderBalances = rawApiBalances
                                .mapValues { it.available }
                                .map { currency, availableBalance ->
                                    tuple(
                                        currency,
                                        tuple(availableBalance, balanceOnOrders.getOrDefault(currency, BigDecimal.ZERO))
                                    )
                                }

                            send(availableAndOnOrderBalances)

                            val currenciesSnapshot = currencyStream.first()

                            launch(start = CoroutineStart.UNDISPATCHED) {
                                super.connection.collect { connected ->
                                    if (!connected) throw DisconnectedException
                                }
                            }

                            launch {
                                super.channelStateFlow(DefaultChannel.AccountNotifications.id).filter { it }.first()
                                streamSynchronizer.unlockStream(LockStreamKeys.BalanceStreamKey)
                            }

                            super.accountNotificationStream.collect { deltaUpdates ->
                                var notifySubscribers = false

                                for (delta in deltaUpdates) {
                                    if (delta is BalanceUpdate) {
                                        if (delta.walletType == WalletType.Exchange) {
                                            val currencyId = delta.currencyId
                                            val currency = currenciesSnapshot._2.getOrNull(currencyId)
                                            val availableOnOrdersBalance = currency?.run { availableAndOnOrderBalances.getOrNull(this) }

                                            if (currency == null || availableOnOrdersBalance == null) {
                                                throw PrivateException.CurrencyOrBalanceNotFound(currencyId, currency, availableOnOrdersBalance)
                                            }

                                            val (available, onOrders) = availableOnOrdersBalance
                                            val newBalance = available + delta.amount
                                            availableAndOnOrderBalances = availableAndOnOrderBalances.put(currency, tuple(newBalance, onOrders))

                                            notifySubscribers = true
                                        }
                                    } else if (delta is LimitOrderCreated) {
                                        val marketId = delta.marketId
                                        val market = marketStream.first()._1.getOrNull(marketId) ?: throw PrivateException.MarketNotFound(marketId)

                                        // 1. Add created order to orders list

                                        allOpenOrders = allOpenOrders.put(
                                            delta.orderId, OpenOrderWithMarket(
                                                delta.orderId,
                                                delta.orderType,
                                                delta.price,
                                                delta.amount,
                                                delta.amount,
                                                delta.amount,
                                                LocalDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")),
                                                false,
                                                market
                                            )
                                        )

                                        val currency: Currency
                                        val deltaOnOrdersAmount: Amount

                                        if (delta.orderType == OrderType.Buy) {
                                            currency = market.baseCurrency
                                            deltaOnOrdersAmount = amountCalculator.fromAmountBuy(delta.amount, delta.price, BigDecimal.ONE)
                                        } else {
                                            currency = market.quoteCurrency
                                            deltaOnOrdersAmount = delta.amount
                                        }

                                        val availableOnOrdersBalance = availableAndOnOrderBalances.getOrNull(currency)
                                            ?: throw PrivateException.BalanceNotFound(currency)

                                        val (available, onOrders) = availableOnOrdersBalance
                                        val newOnOrders = onOrders + deltaOnOrdersAmount
                                        availableAndOnOrderBalances = availableAndOnOrderBalances.put(currency, tuple(available, newOnOrders))

                                        notifySubscribers = true
                                    } else if (delta is OrderUpdate) {
                                        val oldOrder = allOpenOrders.getOrNull(delta.orderId) ?: throw PrivateException.OrderNotFound(delta.orderId)

                                        val oldOrderAmount: BigDecimal
                                        val newOrderAmount: BigDecimal
                                        val balanceCurrency: Currency

                                        if (oldOrder.type == OrderType.Buy) {
                                            oldOrderAmount = amountCalculator.fromAmountBuy(oldOrder.amount, oldOrder.price, BigDecimal.ONE)
                                            newOrderAmount = amountCalculator.fromAmountBuy(delta.newAmount, oldOrder.price, BigDecimal.ONE)
                                            balanceCurrency = oldOrder.market.baseCurrency
                                        } else {
                                            oldOrderAmount = oldOrder.amount
                                            newOrderAmount = delta.newAmount
                                            balanceCurrency = oldOrder.market.quoteCurrency
                                        }

                                        // 1. Adjust open orders map

                                        if (delta.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                            allOpenOrders = allOpenOrders.remove(delta.orderId)
                                        } else {
                                            val newOrder = OpenOrderWithMarket(
                                                oldOrder.orderId,
                                                oldOrder.type,
                                                oldOrder.price,
                                                oldOrder.startingAmount,
                                                delta.newAmount,
                                                newOrderAmount,
                                                oldOrder.date,
                                                oldOrder.margin,
                                                oldOrder.market
                                            )

                                            allOpenOrders = allOpenOrders.put(oldOrder.orderId, newOrder)
                                        }

                                        // 2. Adjust on orders balance

                                        val availableOnOrdersBalance = availableAndOnOrderBalances.getOrNull(balanceCurrency)
                                            ?: throw PrivateException.BalanceNotFound(balanceCurrency)

                                        val (available, onOrders) = availableOnOrdersBalance
                                        val newOnOrders = onOrders - oldOrderAmount + newOrderAmount
                                        availableAndOnOrderBalances = availableAndOnOrderBalances.put(balanceCurrency, tuple(available, newOnOrders))

                                        notifySubscribers = true
                                    }
                                }

                                if (notifySubscribers) this@channelFlow.send(availableAndOnOrderBalances)
                            }
                        }
                    } catch (e: CancellationException) {
                    } catch (e: PrivateException.BalanceNotFound) {
                        if (logger.isDebugEnabled) logger.warn("Can't update balances: ${e.message}")
                    } catch (e: PrivateException.OrderNotFound) {
                        if (logger.isDebugEnabled) logger.warn("Can't update balances: ${e.message}")
                    } catch (e: PrivateException.MarketNotFound) {
                        if (logger.isDebugEnabled) logger.warn("Can't update balances: ${e.message}")
                    } catch (e: PrivateException.CurrencyOrBalanceNotFound) {
                        if (logger.isDebugEnabled) logger.warn("Can't update balances: ${e.message}")
                    } catch (e: DisconnectedException) {
                        streamSynchronizer.lockStream(LockStreamKeys.BalanceStreamKey)
                        delay(1000)
                    } catch (e: Throwable) {
                        streamSynchronizer.lockStream(LockStreamKeys.BalanceStreamKey)
                        if (logger.isDebugEnabled) logger.warn("Can't update balances: ${e.message}")
                        delay(1000)
                    }
                }
            } finally {
                streamSynchronizer.unlockStream(LockStreamKeys.BalanceStreamKey)
            }
        }.share(1, Duration.ofDays(1024), scope)
    }

    val openOrdersStream: Flow<Map<Long, OpenOrderWithMarket>> = run {
        channelFlow {
            try {
                while (isActive) {
                    try {
                        coroutineScope {
                            streamSynchronizer.lockStream(LockStreamKeys.OpenOrdersStreamKey)

                            var allOpenOrders = super.allOpenOrders()
                            send(allOpenOrders)

                            launch(start = CoroutineStart.UNDISPATCHED) {
                                super.connection.collect { connected ->
                                    if (!connected) throw DisconnectedException
                                }
                            }

                            launch {
                                super.channelStateFlow(DefaultChannel.AccountNotifications.id).filter { it }.first()
                                streamSynchronizer.unlockStream(LockStreamKeys.BalanceStreamKey)
                            }

                            super.accountNotificationStream.collect { notifications ->
                                for (update in notifications) {
                                    when (update) {
                                        is LimitOrderCreated -> run {
                                            val marketId = marketStream.first()._1.getOrNull(update.marketId)

                                            if (marketId != null) {
                                                val newOrder = OpenOrderWithMarket(
                                                    update.orderId,
                                                    update.orderType,
                                                    update.price,
                                                    update.amount,
                                                    update.amount,
                                                    update.price * update.amount, // TODO: Incorrect arguments supplied
                                                    update.date,
                                                    false,
                                                    marketId
                                                )

                                                allOpenOrders = allOpenOrders.put(newOrder.orderId, newOrder)
                                                send(allOpenOrders)
                                            } else {
                                                throw PrivateException.MarketNotFound(update.marketId)
                                            }
                                        }
                                        is OrderUpdate -> run {
                                            if (update.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                                allOpenOrders = allOpenOrders.remove(update.orderId)
                                                send(allOpenOrders)
                                            } else {
                                                val order = allOpenOrders.getOrNull(update.orderId)

                                                if (order != null) {
                                                    val newOrder = OpenOrderWithMarket(
                                                        order.orderId,
                                                        order.type,
                                                        order.price,
                                                        order.startingAmount,
                                                        update.newAmount,
                                                        order.total, // TODO: Incorrect value supplied
                                                        order.date,
                                                        order.margin,
                                                        order.market
                                                    )

                                                    allOpenOrders = allOpenOrders.put(order.orderId, newOrder)
                                                    send(allOpenOrders)
                                                } else {
                                                    throw PrivateException.OrderNotFound(update.orderId)
                                                }
                                            }
                                        }
                                        else -> run { /*ignore*/ }
                                    }
                                }
                            }
                        }
                    } catch (e: CancellationException) {
                    } catch (e: PrivateException.OrderNotFound) {
                        if (logger.isDebugEnabled) logger.warn("Can't update open order: ${e.message}")
                    } catch (e: PrivateException.MarketNotFound) {
                        if (logger.isDebugEnabled) logger.warn("Can't update open order: ${e.message}")
                    } catch (e: DisconnectedException) {
                        streamSynchronizer.lockStream(LockStreamKeys.OpenOrdersStreamKey)
                        delay(1000)
                    } catch (e: Throwable) {
                        streamSynchronizer.lockStream(LockStreamKeys.OpenOrdersStreamKey)
                        if (logger.isDebugEnabled) logger.warn("Can't update open order: ${e.message}")
                        delay(1000)
                    }
                }
            } finally {
                streamSynchronizer.unlockStream(LockStreamKeys.OpenOrdersStreamKey)
            }
        }.share(1, Duration.ofDays(1024), scope)
    }

    val dayVolumeStream: Flow<Map<Market, Tuple2<Amount, Amount>>> = run {
        channelFlow {
            while (isActive) {
                try {
                    send(super.dayVolume())
                    delay(3 * 60 * 1000)
                } catch (e: CancellationException) {
                    delay(1000)
                } catch (e: Throwable) {
                    logger.debug { "Can't fetch day volume from Poloniex because ${e.message}" }
                    delay(2000)
                }
            }
        }.share(1, Duration.ofMinutes(20), scope)
    }

    val tradesStatStream: Flow<Map<MarketId, Flow<TradeStat>>> = run {
        channelFlow {
            dayVolumeStream.collect { dayVolumeMap ->
                val marketsMap = marketStream.first()._2
                val map = marketsMap.map { market, marketId ->
                    val amount = dayVolumeMap.getOrNull(market)
                    val amountBase = amount?._1?.divide(BigDecimal(2), 8, RoundingMode.DOWN)
                    val amountQuote = amount?._2?.divide(BigDecimal(2), 8, RoundingMode.DOWN)
                    val buySellStat = TradeStatOrder(
                        baseQuoteAvgAmount = tuple(
                            amountBase ?: BigDecimal.ZERO,
                            amountQuote ?: BigDecimal.ZERO
                        )
                    )
                    val tradeStat = TradeStat(
                        sell = buySellStat,
                        buy = buySellStat
                    )
                    tuple(marketId, flowOf(tradeStat))
                }

                send(map)
            }
        }.share(1, Duration.ofMinutes(20), scope)
    }

    val marketTickerStream: Flow<Map<Market, Ticker>> = run {
        fun mapTicker(m: Market, t: Ticker0): Tuple2<Market, Ticker> {
            return tuple(
                m,
                Ticker(
                    t.id,
                    t.last,
                    t.lowestAsk,
                    t.highestBid,
                    t.percentChange,
                    t.baseVolume,
                    t.quoteVolume,
                    t.isFrozen,
                    t.high24hr,
                    t.low24hr
                )
            )
        }

        channelFlow {
            while (isActive) {
                try {
                    var allTickers = super.ticker().map(::mapTicker)
                    send(allTickers)

                    coroutineScope {
                        launch(start = CoroutineStart.UNDISPATCHED) {
                            super.connection.collect { connected ->
                                if (!connected) throw DisconnectedException
                            }
                        }

                        super.tickerStream.collect { ticker ->
                            val marketId = marketStream.first()._1.getOrNull(ticker.id)

                            if (marketId != null) {
                                allTickers = allTickers.put(marketId, ticker)
                                send(allTickers)
                            } else {
                                throw PrivateException.MarketNotFound(ticker.id)
                            }
                        }
                    }
                } catch (e: CancellationException) {
                } catch (e: PrivateException.MarketNotFound) {
                    if (logger.isDebugEnabled) logger.warn(e.message)
                } catch (e: DisconnectedException) {
                    delay(1000)
                } catch (e: Throwable) {
                    logger.warn(e.message)
                    delay(1000)
                }
            }
        }.share(1)
    }

    val orderBooksStream: Flow<OrderBookDataMap> = run {
        marketStream.map { marketInfo ->
            marketInfo._1.keySet().toVavrStream()
                .map { marketId ->
                    val newBookStream = marketOrderBook.getOrPut(marketId) {
                        orderBookStream(marketId)
                            .map { (book, update) -> OrderBookData(marketInfo._1.get(marketId).get(), marketId, book, update) }
                            .share(1, Duration.ofMinutes(2), scope)
                    }
                    tuple(marketId, newBookStream)
                }
                .toMap { it }
        }.share(1, Duration.ofMinutes(30), scope)
    }

    val orderBooksPollingStream: Flow<Map<Market, OrderBookSnapshot>> = run {
        channelFlow {
            while (isActive) {
                try {
                    val orderBooks = super.orderBooks(market = null, depth = 20)
                    send(orderBooks)
                    delay(30000)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: UnknownHostException) {
                    delay(2000)
                } catch (e: ReadTimeoutException) {
                    delay(2000)
                } catch (e: WriteTimeoutException) {
                    delay(2000)
                } catch (e: IOException) {
                    delay(2000)
                } catch (e: ConnectException) {
                    delay(2000)
                } catch (e: SocketException) {
                    delay(2000)
                } catch (e: InvalidMarketException) {
                    logger.warn(e.message)
                    delay(1000)
                } catch (e: InvalidDepthException) {
                    logger.warn(e.message)
                    delay(1000)
                } catch (e: Throwable) {
                    delay(1000)
                    logger.error("Error occurred in orderBooksPolling stream: ${e.message}")
                }
            }
        }.share(1, Duration.ofSeconds(30), scope)
    }

    val feeStream: Flow<FeeMultiplier> = run {
        suspend fun fetchFee(): FeeMultiplier {
            val fee = super.feeInfo()
            return FeeMultiplier(fee.makerFee.oneMinusAdjPoloniex, fee.takerFee.oneMinusAdjPoloniex)
        }

        channelFlow {
            while (isActive) {
                try {
                    send(fetchFee())
                    break
                } catch (e: CancellationException) {
                    delay(1000)
                } catch (e: Throwable) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch fee from Poloniex because ${e.message}")
                    delay(2000)
                }
            }

            // TODO: How to get fee instantly without polling ?
            while (isActive) {
                delay(10 * 60 * 1000)

                try {
                    send(fetchFee())
                } catch (e: CancellationException) {
                    delay(1000)
                } catch (e: Throwable) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch fee from Poloniex because ${e.message}")
                }
            }
        }.share(1, Duration.ofDays(1024), scope)
    }

    fun generateOrderId(): Long {
        return orderIdGenerator.incrementAndGet()
    }

    private fun orderBookStream(marketId: MarketId): Flow<Tuple2<PriceAggregatedBook, List<OrderBookNotification>>> = channelFlow {
        while (true) {
            try {
                coroutineScope {
                    var book = PriceAggregatedBook()

                    launch(start = CoroutineStart.UNDISPATCHED) {
                        super.connection.collect { connected ->
                            if (!connected) throw DisconnectedException
                        }
                    }

                    subscribeTo(marketId, jacksonTypeRef<List<OrderBookNotification>>()).collect { notificationList ->
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
                        }

                        if (notificationList.size() > 0) {
                            send(tuple(book, notificationList))
                        }
                    }
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: DisconnectedException) {
                delay(1000)
            } catch (e: Throwable) {
                logger.debug("Error occurred in orderBookStream of market $marketId: ${e.message}")
                delay(1000)
            }
        }
    }

    override suspend fun placeLimitOrder(market: Market, orderType: OrderType, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?, clientOrderId: Long?): LimitOrderResult {
        return streamSynchronizer.withLockFunc {
            super.placeLimitOrder(market, orderType, price, amount, tpe, clientOrderId)
        }
    }

    override suspend fun cancelOrder(orderId: Long, orderIdType: CancelOrderIdType): CancelOrder {
        return streamSynchronizer.withLockFunc {
            super.cancelOrder(orderId, orderIdType)
        }
    }

    override suspend fun cancelAllOrders(market: Market?): CancelAllOrders {
        return streamSynchronizer.withLockFunc {
            super.cancelAllOrders(market)
        }
    }

    override suspend fun moveOrder(orderId: Long, price: Price, amount: Amount?, orderType: BuyOrderType?, clientOrderId: Long?): MoveOrderResult {
        return streamSynchronizer.withLockFunc {
            super.moveOrder(orderId, price, amount, orderType, clientOrderId)
        }
    }

    companion object {
        private object LockStreamKeys {
            const val BalanceStreamKey = "balanceStream"
            const val OpenOrdersStreamKey = "openOrdersStream"
        }

        private object PrivateException {
            class BalanceNotFound(currency: Currency) : Throwable("Can't find balance for currency $currency", null, true, false)
            class OrderNotFound(orderId: Long) : Throwable("Order $orderId not found in local cache", null, true, false)
            class MarketNotFound(marketId: MarketId) : Throwable("Can't find market for marketId $marketId", null, true, false)
            class CurrencyOrBalanceNotFound(currencyId: Int, currency: Currency?, availableOnOrdersBalance: Tuple2<BigDecimal, BigDecimal>?) :
                Throwable("Currency $currency for $currencyId or balance $availableOnOrdersBalance not found ", null, true, false)
        }

        object PublicException {
            class MarketIdNotFound(market: Market) : Throwable("Market ID is not found found in marketsStream for market $market", null, true, false)
            class OrderBookNotFound(marketId: MarketId) : Throwable("Order book for $marketId not found", null, true, false)
        }

        class StreamSynchronizerCounter(defaultValue: Int) {
            private val _counter = MutableStateFlow(defaultValue)
            private val mutex = Mutex()
            val counter: StateFlow<Int> get() = _counter

            suspend fun inc() {
                withContext(NonCancellable) {
                    mutex.withLock {
                        _counter.value++
                    }
                }
            }

            suspend fun dec() {
                withContext(NonCancellable) {
                    mutex.withLock {
                        _counter.value--
                    }
                }
            }
        }

        class StreamSynchronizer {
            private val streamsLockMap = ConcurrentHashMap<String, Unit>()

            private val streamsFlow = StreamSynchronizerCounter(0)
            private val functionsFlow = StreamSynchronizerCounter(0)

            suspend fun lockFunc() {
                while (true) {
                    try {
                        streamsFlow.counter.filter { it == 0 }.first()
                    } finally {
                        functionsFlow.inc()
                    }
                    if (streamsFlow.counter.value == 0) break else unlockFunc()
                }
            }

            suspend fun unlockFunc() {
                functionsFlow.dec()
            }

            suspend fun lockStream(key: String) {
                val alreadyLocked = streamsLockMap.putIfAbsent(key, Unit)
                if (alreadyLocked == null) streamsFlow.inc()
                functionsFlow.counter.filter { it == 0 }.first()
            }

            suspend fun unlockStream(key: String) {
                val removedValue = streamsLockMap.remove(key)
                if (removedValue != null) streamsFlow.dec()
            }

            suspend inline fun <T> withLockFunc(action: () -> T): T {
                lockFunc()
                try {
                    return action()
                } finally {
                    unlockFunc()
                }
            }

            suspend inline fun <T> withLockStream(key: String, action: () -> T): T {
                lockStream(key)
                try {
                    return action()
                } finally {
                    unlockStream(key)
                }
            }
        }
    }
}

suspend fun ExtendedPoloniexApi.getMarketId(market: Market): MarketId? {
    return marketStream.first()._2.getOrNull(market)
}

suspend fun ExtendedPoloniexApi.getMarket(marketId: MarketId): Market? {
    return marketStream.first()._1.getOrNull(marketId)
}

@Throws(
    ExtendedPoloniexApi.Companion.PublicException.MarketIdNotFound::class,
    ExtendedPoloniexApi.Companion.PublicException.OrderBookNotFound::class
)
suspend fun ExtendedPoloniexApi.getOrderBookFlowBy(market: Market): Flow<OrderBookAbstract> {
    val marketId = getMarketId(market) ?: throw ExtendedPoloniexApi.Companion.PublicException.MarketIdNotFound(market)
    val data = orderBooksStream.first().getOrNull(marketId) ?: throw ExtendedPoloniexApi.Companion.PublicException.OrderBookNotFound(marketId)
    return data.map { it.book }
}
