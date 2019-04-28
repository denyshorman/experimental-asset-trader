package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.InvalidOrderNumberException
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.TransactionFailedException
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import io.vavr.Tuple2
import io.vavr.collection.Queue
import io.vavr.collection.List
import io.vavr.collection.Array
import io.vavr.collection.TreeSet
import io.vavr.collection.Vector
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.consumeEach
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.reactor.flux
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration
import java.util.*
import kotlin.Comparator

class PoloniexTrader(private val poloniexApi: PoloniexApi) {
    private val logger = KotlinLogging.logger {}

    private val trigger = TriggerStreams()
    private val raw = RawStreams(trigger, poloniexApi)
    val data = DataStreams(raw, trigger, poloniexApi)
    val indicators = IndicatorStreams(data)

    var primaryCurrencies: List<Currency> = list("USDT", "USDC")

    private val unfilledMarkets = HashMap<Currency, Array<TranIntentMarket>>()
    private val unfilledMarketsMutex = Mutex()

    private suspend fun addUnfilledMarkets(currentMarketIdx: Int, markets: Array<TranIntentMarket>) {
        unfilledMarketsMutex.withLock {
            // TODO: Add to database
            val currMarket = markets[currentMarketIdx]
            unfilledMarkets.put(currMarket.fromCurrency, markets)
        }
    }

    private suspend fun getAndRemoveUnfilledMarkets(fromCurrency: Currency): Array<TranIntentMarket>? {
        unfilledMarketsMutex.withLock {
            // TODO: Get and remove from database
            val markets = unfilledMarkets.get(fromCurrency)
            if (markets != null) unfilledMarkets.remove(fromCurrency)
            return markets
        }
    }

    fun start(): Mono<Unit> = FlowScope.mono {
        logger.info("Start trading")

        val tranIntentScope =
            CoroutineScope(Schedulers.parallel().asCoroutineDispatcher() + SupervisorJob(coroutineContext[Job]))

        try {
            val tickerFlow = Flux.interval(Duration.ofSeconds(60))
                .startWith(0)
                .onBackpressureDrop()

            tickerFlow.consumeEach {
                val (startCurrency, requestedAmount) = requestBalanceForTransaction() ?: return@consumeEach

                val bestPath = try {
                    selectBestPath(startCurrency, requestedAmount) ?: return@consumeEach
                } finally {
                    releaseBalance(startCurrency, requestedAmount)
                }

                startPathTransaction(bestPath, tranIntentScope)
            }
        } finally {
            tranIntentScope.cancel()
        }
    }

    private suspend fun selectBestPath(startCurrency: Currency, amount: Amount): ExhaustivePath? {
        val allPaths = withContext(Dispatchers.IO) {
            indicators.getPaths(PathsSettings(amount, primaryCurrencies)).awaitFirst()
        }

        return selectBestPathForTransaction(allPaths, amount, startCurrency)
    }

    private fun startPathTransaction(bestPath: ExhaustivePath, TranIntentScope: CoroutineScope): Job {
        val markets = prepareMarketsForIntent(bestPath)
        val intent = TransactionIntent(markets, 0, TranIntentScope)
        return intent.start()
    }

    private fun prepareMarketsForIntent(bestPath: ExhaustivePath): Array<TranIntentMarket> {
        val markets = bestPath.chain.mapIndexed { index, order ->
            when (order) {
                is InstantOrder -> run {
                    if (index == 0) {
                        TranIntentMarketPartiallyCompleted(
                            order.market,
                            OrderSpeed.Instant,
                            order.market.tpe(order.fromCurrency)!!,
                            order.fromAmount
                        )
                    } else {
                        TranIntentMarketPredicted(
                            order.market,
                            OrderSpeed.Instant,
                            order.market.tpe(order.fromCurrency)!!
                        )
                    }
                }
                is DelayedOrder -> run {
                    if (index == 0) {
                        TranIntentMarketPartiallyCompleted(
                            order.market,
                            OrderSpeed.Delayed,
                            order.market.tpe(order.fromCurrency)!!,
                            order.fromAmount
                        )
                    } else {
                        TranIntentMarketPredicted(
                            order.market,
                            OrderSpeed.Delayed,
                            order.market.tpe(order.fromCurrency)!!
                        )
                    }
                }
            }
        }

        return Array.ofAll(markets)
    }

    private fun selectBestPathForTransaction(
        allPaths: TreeSet<ExhaustivePath>?,
        requestedAmount: Amount,
        startCurrency: Currency
    ): ExhaustivePath? {
        TODO("implement selectBestPathForTransaction")
    }

    private suspend fun findNewPath(
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>
    ): Array<TranIntentMarket>? {
        val bestPath = selectBestPath(fromCurrency, fromAmount) ?: return null
        return prepareMarketsForIntent(bestPath)
    }

    private fun releaseBalance(startCurrency: Currency, requestedAmount: Amount) {
        TODO("implement releaseBalance")
    }

    private fun requestBalanceForTransaction(): Tuple2<Currency, Amount>? {
        TODO("implement requestBalanceForTransaction")
    }

    private inner class TransactionIntent(
        val markets: Array<TranIntentMarket>,
        val marketIdx: Int,
        val TranIntentScope: CoroutineScope
    ) {
        fun start(): Job = TranIntentScope.launch {
            val currentMarket = markets[marketIdx] as TranIntentMarketPartiallyCompleted
            val orderBookFlow = data.getOrderBookFlowBy(currentMarket.market)
            val feeFlow = data.fee
            val newMarketIdx = marketIdx + 1

            when (currentMarket.orderSpeed) {
                OrderSpeed.Instant -> run {
                    val trades = buySellInstantly(
                        currentMarket.market,
                        currentMarket.fromCurrency,
                        currentMarket.fromAmount,
                        orderBookFlow,
                        feeFlow
                    )

                    val (currentUpdatedMarkets, committedMarkets) = splitWithNewTrades(markets, marketIdx, trades)

                    addUnfilledMarkets(marketIdx, currentUpdatedMarkets)

                    if (newMarketIdx != markets.length()) {
                        val newIntent = TransactionIntent(
                            committedMarkets,
                            newMarketIdx,
                            TranIntentScope
                        )
                        newIntent.start()
                    }
                }
                OrderSpeed.Delayed -> run {
                    var updatedMarkets = markets
                    val marketsChannel = ConflatedBroadcastChannel(updatedMarkets)
                    val profitMonitoringJob = startProfitMonitoring(marketsChannel)
                    profitMonitoringJob.start()

                    buySellDelayed(
                        currentMarket.orderType,
                        currentMarket.market,
                        currentMarket.fromAmount,
                        orderBookFlow
                    )
                        .buffer(Duration.ofSeconds(10))
                        .map { Array.ofAll(it) }
                        .consumeEach { trades ->
                            val marketSplit = splitWithNewTrades(updatedMarkets, marketIdx, trades)

                            updatedMarkets = marketSplit._1
                            marketsChannel.send(updatedMarkets)

                            if (newMarketIdx == markets.length()) return@consumeEach

                            val committedMarkets = marketSplit._2

                            val newIntent = TransactionIntent(
                                committedMarkets,
                                newMarketIdx,
                                TranIntentScope
                            )

                            newIntent.start()
                        }

                    profitMonitoringJob.cancelAndJoin()
                }
            }
        }

        private suspend fun buySellInstantly(
            market: Market,
            fromCurrency: Currency,
            fromCurrencyAmount: Amount,
            orderBookFlow: Flux<OrderBookAbstract>,
            feeFlow: Flux<FeeMultiplier>
        ): Array<BareTrade> {
            val orderType = if (market.baseCurrency == fromCurrency) {
                OrderType.Buy
            } else {
                OrderType.Sell
            }

            val trades = LinkedList<BareTrade>()
            var unfilledAmount = fromCurrencyAmount

            while (unfilledAmount.compareTo(BigDecimal.ZERO) != 0) {
                val simulatedTrades = simulateInstantTrades(unfilledAmount, orderType, orderBookFlow, feeFlow)

                // TODO: Investigate break
                val firstSimulatedTrade = simulatedTrades._2.headOption().orNull ?: break

                val expectQuoteAmount = if (orderType == OrderType.Buy) {
                    calcQuoteAmount(firstSimulatedTrade.quoteAmount, firstSimulatedTrade.price)
                } else {
                    firstSimulatedTrade.quoteAmount
                }

                // TODO: Handle all posible errors
                // TODO: Investigate FillOrKill and ImmediateOrCancel
                val transaction = if (orderType == OrderType.Buy) {
                    poloniexApi.buy(market, firstSimulatedTrade.price, expectQuoteAmount, BuyOrderType.FillOrKill)
                        .awaitSingle()
                } else {
                    poloniexApi.sell(market, firstSimulatedTrade.price, expectQuoteAmount, BuyOrderType.FillOrKill)
                        .awaitSingle()
                }

                for (trade in transaction.trades) {
                    unfilledAmount -= if (orderType == OrderType.Buy) {
                        buyBaseAmount(trade.amount, trade.price)
                    } else {
                        sellQuoteAmount(trade.amount)
                    }

                    trades.addLast(BareTrade(trade.amount, trade.price, transaction.feeMultiplier))
                }
            }

            return Array.ofAll(trades)
        }

        private fun buySellDelayed(
            orderType: OrderType,
            market: Market,
            fromCurrencyAmount: Amount,
            orderBook: Flux<OrderBookAbstract>
        ): Flux<BareTrade> = FlowScope.flux {
            val tradesChannel: ProducerScope<BareTrade> = this
            val unfilledAmountChannel = ConflatedBroadcastChannel<Amount>()
            val latestOrderIdsChannel = ConflatedBroadcastChannel<Queue<Long>>()

            // Trades monitoring
            launch {
                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Starting trades consumption...")

                var unfilledAmount = fromCurrencyAmount

                raw.accountNotifications.consumeEach { trade ->
                    if (trade !is TradeNotification) return@consumeEach

                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trade received: $trade")

                    val orderIds = latestOrderIdsChannel.valueOrNull ?: return@consumeEach

                    for (orderId in orderIds.reverseIterator()) {
                        if (orderId != trade.orderId) continue

                        if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trade matched. Sending trade to the client..")

                        val filledAmount: Amount

                        if (orderType == OrderType.Buy) {
                            filledAmount = buyBaseAmount(trade.amount, trade.price)
                            unfilledAmount -= filledAmount
                        } else {
                            filledAmount = sellQuoteAmount(trade.amount)
                            unfilledAmount -= filledAmount
                        }

                        unfilledAmountChannel.send(unfilledAmount)
                        tradesChannel.send(BareTrade(trade.amount, trade.price, trade.feeMultiplier))

                        break
                    }
                }
            }

            // Place - Move order loop
            launch {
                var orderJob: Job? = null

                unfilledAmountChannel.consumeEach { unfilledAmount ->
                    if (unfilledAmount.compareTo(BigDecimal.ZERO) == 0) {
                        if (orderJob != null) {
                            orderJob!!.cancelAndJoin()
                            orderJob = null
                        }
                    } else {
                        if (orderJob == null) {
                            orderJob = launch {
                                val maxOrdersCount = 16
                                var latestOrderIds = Queue.empty<Long>()
                                var amountValue = unfilledAmountChannel.value

                                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Placing new order with amount $amountValue...")

                                val placeOrderResult = placeOrder(market, orderBook, orderType, amountValue)

                                var lastOrderId = placeOrderResult._1
                                var prevPrice = placeOrderResult._2

                                latestOrderIds = latestOrderIds.append(lastOrderId)
                                if (latestOrderIds.size() > maxOrdersCount) latestOrderIds = latestOrderIds.drop(1)
                                latestOrderIdsChannel.send(latestOrderIds)

                                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] New order placed: $lastOrderId, $prevPrice")

                                orderBook.onBackpressureLatest().consumeEach orderBookLabel@{ book ->
                                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trying to move order $lastOrderId...")

                                    amountValue = unfilledAmountChannel.value

                                    var moveOrderResult: Tuple2<Long, Price>? = null

                                    while (isActive) {
                                        try {
                                            moveOrderResult =
                                                moveOrder(market, book, orderType, lastOrderId, prevPrice, amountValue)
                                                    ?: return@orderBookLabel

                                            break
                                        } catch (e: TransactionFailedException) {
                                            continue
                                        } catch (e: InvalidOrderNumberException) {
                                            cancel()
                                        }
                                    }

                                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Order $lastOrderId moved to ${moveOrderResult!!._1} with price ${moveOrderResult._2}")

                                    lastOrderId = moveOrderResult!!._1
                                    prevPrice = moveOrderResult._2

                                    latestOrderIds = latestOrderIds.append(lastOrderId)
                                    if (latestOrderIds.size() > maxOrdersCount) latestOrderIds = latestOrderIds.drop(1)
                                    latestOrderIdsChannel.send(latestOrderIds)
                                }
                            }
                        }
                    }
                }
            }

            // Completion monitoring
            launch {
                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Start monitoring for all trades completion...")

                unfilledAmountChannel.consumeEach {
                    if (it.compareTo(BigDecimal.ZERO) == 0) {
                        if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Amount = 0 => all trades received. Closing...")
                        tradesChannel.close()
                        return@launch
                    }
                }
            }
        }

        private suspend fun moveOrder(
            market: Market,
            book: OrderBookAbstract,
            orderType: OrderType,
            lastOrderId: Long,
            myPrice: Price,
            amountValue: BigDecimal
        ): Tuple2<Long, Price>? {
            val newPrice = run {
                val primaryBook: SubOrderBook
                val secondaryBook: SubOrderBook
                val priceComparator: Comparator<Price>
                val moveToOnePoint: (Price) -> Price

                when (orderType) {
                    OrderType.Buy -> run {
                        primaryBook = book.bids
                        secondaryBook = book.asks
                        priceComparator = Comparator { bookPrice, myPrice ->
                            when {
                                bookPrice > myPrice -> 1
                                bookPrice.compareTo(myPrice) == 0 -> 0
                                else -> -1
                            }
                        }
                        moveToOnePoint = { price -> price.cut8add1 }
                    }
                    OrderType.Sell -> run {
                        primaryBook = book.asks
                        secondaryBook = book.bids
                        priceComparator = Comparator { bookPrice, myPrice ->
                            when {
                                bookPrice < myPrice -> 1
                                bookPrice.compareTo(myPrice) == 0 -> 0
                                else -> -1
                            }
                        }
                        moveToOnePoint = { price -> price.cut8minus1 }
                    }
                }

                val bookPrice1 = primaryBook.headOption().map { it._1 }.orNull
                    ?: throw Exception("OrderBookEmptyException($orderType)")
                val myPositionInBook = priceComparator.compare(bookPrice1, myPrice)

                if (myPositionInBook == 1) {
                    // I am on second position
                    val anotherWorkerOnTheSameMarket = data.orderBookOrders.awaitFirst()
                        .contains(BookOrder(market, bookPrice1, orderType))

                    if (anotherWorkerOnTheSameMarket) {
                        bookPrice1
                    } else {
                        var price = moveToOnePoint(bookPrice1)
                        val ask = secondaryBook.headOption().map { it._1 }.orNull
                        if (ask != null && ask.compareTo(price) == 0) price = bookPrice1
                        price
                    }
                } else {
                    // I am on first position
                    null // Ignore possible price gaps
                }
            } ?: return null

            val moveOrderResult = poloniexApi.moveOrder(
                lastOrderId,
                newPrice,
                amountValue,
                BuyOrderType.PostOnly
            ).awaitSingle()

            return tuple(moveOrderResult.orderId, newPrice)
        }

        private suspend fun placeOrder(
            market: Market,
            orderBook: Flux<OrderBookAbstract>,
            orderType: OrderType,
            amountValue: BigDecimal
        ): Tuple2<Long, Price> {
            val primaryBook: SubOrderBook
            val secondaryBook: SubOrderBook
            val moveToOnePoint: (Price) -> Price

            val book = orderBook.awaitFirst()
            val bookOrders = data.orderBookOrders.awaitFirst()

            when (orderType) {
                OrderType.Buy -> run {
                    primaryBook = book.bids
                    secondaryBook = book.asks
                    moveToOnePoint = { price -> price.cut8add1 }
                }
                OrderType.Sell -> run {
                    primaryBook = book.asks
                    secondaryBook = book.bids
                    moveToOnePoint = { price -> price.cut8minus1 }
                }
            }

            val price = run {
                val topPricePrimary = primaryBook.headOption().map { it._1 }.orNull ?: return@run null

                val anotherWorkerOnTheSameMarket = bookOrders.contains(BookOrder(market, topPricePrimary, orderType))

                if (anotherWorkerOnTheSameMarket) {
                    topPricePrimary
                } else {
                    val newPrice = moveToOnePoint(topPricePrimary)
                    val topPriceSecondary = secondaryBook.headOption().map { it._1 }.orNull
                    if (topPriceSecondary != null && topPriceSecondary.compareTo(newPrice) == 0) topPricePrimary else newPrice
                }
            } ?: throw Exception("OrderBookEmptyException($orderType)")

            val result = when (orderType) {
                OrderType.Buy -> poloniexApi.buy(market, price, amountValue, BuyOrderType.PostOnly)
                OrderType.Sell -> poloniexApi.sell(market, price, amountValue, BuyOrderType.PostOnly)
            }.awaitSingle()

            return tuple(result.orderId, price)
        }

        // TODO: Replace with sequence
        private suspend fun simulateInstantTrades(
            fromAmount: Amount,
            orderType: OrderType,
            orderBookFlow: Flux<OrderBookAbstract>,
            feeFlow: Flux<FeeMultiplier>
        ): Tuple2<Amount, Vector<BareTrade>> {
            val orderBook = orderBookFlow.awaitFirst()
            val fee = feeFlow.awaitFirst()

            var trades = Vector.empty<BareTrade>()
            var unusedFromAmount = fromAmount

            if (orderType == OrderType.Buy) {
                if (orderBook.asks.length() == 0) throw OrderBookEmptyException(SubBookType.Sell)

                for ((basePrice, quoteAmount) in orderBook.asks) {
                    val availableFromAmount = buyBaseAmount(quoteAmount, basePrice)

                    if (unusedFromAmount <= availableFromAmount) {
                        val tradeQuoteAmount = calcQuoteAmount(unusedFromAmount, basePrice)
                        trades = trades.append(BareTrade(basePrice, tradeQuoteAmount, fee.taker))
                        unusedFromAmount = BigDecimal.ZERO
                        break
                    } else {
                        unusedFromAmount -= availableFromAmount
                        trades = trades.append(BareTrade(basePrice, quoteAmount, fee.taker))
                    }
                }
            } else {
                if (orderBook.bids.length() == 0) throw OrderBookEmptyException(SubBookType.Buy)

                for ((basePrice, quoteAmount) in orderBook.bids) {
                    if (unusedFromAmount <= quoteAmount) {
                        trades = trades.append(BareTrade(basePrice, unusedFromAmount, fee.taker))
                        unusedFromAmount = BigDecimal.ZERO
                        break
                    } else {
                        unusedFromAmount -= sellQuoteAmount(quoteAmount)
                        trades = trades.append(BareTrade(basePrice, quoteAmount, fee.taker))
                    }
                }
            }

            return tuple(unusedFromAmount, trades)
        }

        private fun CoroutineScope.startProfitMonitoring(updatedMarketsChannel: ConflatedBroadcastChannel<Array<TranIntentMarket>>): Job {
            val parentJob = coroutineContext[Job]

            return launch(context = Job(), start = CoroutineStart.LAZY) {
                while (isActive) {
                    delay(10000)
                    val updatedMarkets = updatedMarketsChannel.value

                    val fromAmount = updatedMarkets.first().fromAmount(updatedMarkets, 0)
                    val targetAmount = updatedMarkets.last().targetAmount(updatedMarkets, updatedMarkets.length() - 1)

                    if (fromAmount > targetAmount) {
                        parentJob?.cancelAndJoin()
                        val fromCurrency =
                            (updatedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted).fromCurrency
                        var bestPath: Array<TranIntentMarket>? = null
                        while (isActive) {
                            bestPath = findNewPath(fromCurrency, fromAmount, primaryCurrencies)
                            if (bestPath != null) break;
                            delay(60000)
                        }
                        val changedMarkets = updateMarketsWithBestPath(updatedMarkets, marketIdx, bestPath!!)
                        val newIntent = TransactionIntent(
                            changedMarkets,
                            marketIdx,
                            TranIntentScope
                        )
                        newIntent.start()
                    }
                }
            }
        }

        private fun updateMarketsWithBestPath(
            markets: Array<TranIntentMarket>,
            marketIdx: Int,
            bestPath: Array<TranIntentMarket>
        ): Array<TranIntentMarket> {
            return markets.dropRight(markets.length() - marketIdx).appendAll(bestPath)
        }

        private fun splitWithNewTrades(
            markets: Array<TranIntentMarket>,
            currentMarketIdx: Int,
            trades: Array<BareTrade>
        ): Tuple2<Array<TranIntentMarket>, Array<TranIntentMarket>> {
            val selectedMarket = markets[currentMarketIdx] as TranIntentMarketPartiallyCompleted

            var updatedMarkets = markets
            var committedMarkets = markets

            // Commit current market and prepare next

            val marketCompleted = TranIntentMarketCompleted(
                selectedMarket.market,
                selectedMarket.orderSpeed,
                selectedMarket.fromCurrencyType,
                trades
            )

            committedMarkets = committedMarkets.update(currentMarketIdx, marketCompleted)

            val nextMarketIdx = currentMarketIdx + 1

            if (nextMarketIdx < markets.length()) {
                val nextMarket = markets[nextMarketIdx]
                val nextMarketInit = TranIntentMarketPartiallyCompleted(
                    nextMarket.market,
                    nextMarket.orderSpeed,
                    nextMarket.fromCurrencyType,
                    marketCompleted.targetAmount
                )
                committedMarkets = committedMarkets.update(nextMarketIdx, nextMarketInit)
            }

            // Update current market

            val updatedMarket = TranIntentMarketPartiallyCompleted(
                selectedMarket.market,
                selectedMarket.orderSpeed,
                selectedMarket.fromCurrencyType,
                selectedMarket.fromAmount - marketCompleted.fromAmount
            )

            updatedMarkets = updatedMarkets.update(currentMarketIdx, updatedMarket)


            // Split trades of previous markets
            var i = currentMarketIdx - 1

            while (i >= 0) {
                val m = markets[i] as TranIntentMarketCompleted

                val updatedTrades = mutableListOf<BareTrade>()
                val committedTrades = mutableListOf<BareTrade>()

                var targetAmount = (committedMarkets[i + 1] as TranIntentMarketCompleted).fromAmount

                for (trade in m.trades) {
                    if (m.orderType == OrderType.Buy) {
                        val bought = buyQuoteAmount(trade.quoteAmount, trade.feeMultiplier)

                        if (bought <= targetAmount) {
                            committedTrades.add(trade)
                            targetAmount -= bought
                        } else {
                            if (targetAmount.compareTo(BigDecimal.ZERO) == 0) {
                                updatedTrades.add(trade)
                            } else {
                                val commitQuoteAmount = run {
                                    var expectedQuote = targetAmount.divide(trade.feeMultiplier, 8, RoundingMode.DOWN)
                                    val boughtQuote = buyQuoteAmount(expectedQuote, trade.feeMultiplier)
                                    if (boughtQuote.compareTo(targetAmount) != 0) {
                                        expectedQuote = targetAmount.divide(trade.feeMultiplier, 8, RoundingMode.UP)
                                    }
                                    expectedQuote
                                }
                                updatedTrades.add(
                                    BareTrade(
                                        trade.quoteAmount - commitQuoteAmount,
                                        trade.price,
                                        trade.feeMultiplier
                                    )
                                )
                                committedTrades.add(BareTrade(commitQuoteAmount, trade.price, trade.feeMultiplier))
                                targetAmount = BigDecimal.ZERO
                            }
                        }
                    } else {
                        val sold = sellBaseAmount(trade.quoteAmount, trade.price, trade.feeMultiplier)

                        if (sold <= targetAmount) {
                            committedTrades.add(trade)
                            targetAmount -= sold
                        } else {
                            if (targetAmount.compareTo(BigDecimal.ZERO) == 0) {
                                updatedTrades.add(trade)
                            } else {
                                val commitQuoteAmount = run {
                                    val tfd: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                                        targetAmount.divide(
                                            trade.feeMultiplier,
                                            8,
                                            RoundingMode.DOWN
                                        )
                                    }
                                    val tfu: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                                        targetAmount.divide(
                                            trade.feeMultiplier,
                                            8,
                                            RoundingMode.UP
                                        )
                                    }
                                    val qdd: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                                        tfd.divide(
                                            trade.price,
                                            8,
                                            RoundingMode.DOWN
                                        )
                                    }
                                    val qdu: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                                        tfd.divide(
                                            trade.price,
                                            8,
                                            RoundingMode.UP
                                        )
                                    }
                                    val qud: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                                        tfu.divide(
                                            trade.price,
                                            8,
                                            RoundingMode.DOWN
                                        )
                                    }
                                    val quu: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                                        tfu.divide(
                                            trade.price,
                                            8,
                                            RoundingMode.UP
                                        )
                                    }
                                    when {
                                        sellBaseAmount(
                                            qdd,
                                            trade.price,
                                            trade.feeMultiplier
                                        ).compareTo(targetAmount) == 0 -> qdd
                                        sellBaseAmount(
                                            qdu,
                                            trade.price,
                                            trade.feeMultiplier
                                        ).compareTo(targetAmount) == 0 -> qdu
                                        sellBaseAmount(
                                            qud,
                                            trade.price,
                                            trade.feeMultiplier
                                        ).compareTo(targetAmount) == 0 -> qud
                                        sellBaseAmount(
                                            quu,
                                            trade.price,
                                            trade.feeMultiplier
                                        ).compareTo(targetAmount) == 0 -> quu
                                        else -> throw Exception("Can't find quote amount that matches target price")
                                    }
                                }
                                updatedTrades.add(
                                    BareTrade(
                                        trade.quoteAmount - commitQuoteAmount,
                                        trade.price,
                                        trade.feeMultiplier
                                    )
                                )
                                committedTrades.add(BareTrade(commitQuoteAmount, trade.price, trade.feeMultiplier))
                                targetAmount = BigDecimal.ZERO
                            }
                        }
                    }
                }

                val updated = TranIntentMarketCompleted(
                    m.market,
                    m.orderSpeed,
                    m.fromCurrencyType,
                    Array.ofAll(updatedTrades)
                )

                val committed = TranIntentMarketCompleted(
                    m.market,
                    m.orderSpeed,
                    m.fromCurrencyType,
                    Array.ofAll(committedTrades)
                )

                updatedMarkets = updatedMarkets.update(i, updated)
                committedMarkets = committedMarkets.update(i, committed)

                i--
            }

            return tuple(updatedMarkets, committedMarkets)
        }

        private suspend fun TranIntentMarketPredicted.predictedFromAmount(
            markets: Array<TranIntentMarket>,
            idx: Int
        ): Amount {
            val prevIdx = idx - 1
            return when (val prevTran = markets[prevIdx]) {
                is TranIntentMarketCompleted -> prevTran.targetAmount
                is TranIntentMarketPartiallyCompleted -> prevTran.predictedTargetAmount()
                is TranIntentMarketPredicted -> prevTran.predictedTargetAmount(markets, prevIdx)
            }
        }

        private suspend fun TranIntentMarketPredicted.predictedTargetAmount(
            markets: Array<TranIntentMarket>,
            idx: Int
        ): Amount {
            val fee = data.fee.awaitFirst()
            val orderBook = data.getOrderBookFlowBy(market).awaitFirst()
            val fromAmount = predictedFromAmount(markets, idx)

            return if (orderSpeed == OrderSpeed.Instant) {
                getInstantOrderTargetAmount(orderType, fromAmount, fee.taker, orderBook)
            } else {
                getDelayedOrderTargetAmount(orderType, fromAmount, fee.maker, orderBook)
            }
        }

        private suspend fun TranIntentMarketPartiallyCompleted.predictedTargetAmount(): Amount {
            val fee = data.fee.awaitFirst()
            val orderBook = data.getOrderBookFlowBy(market).awaitFirst()

            return if (orderSpeed == OrderSpeed.Instant) {
                getInstantOrderTargetAmount(orderType, fromAmount, fee.taker, orderBook)
            } else {
                getDelayedOrderTargetAmount(orderType, fromAmount, fee.maker, orderBook)
            }
        }

        private suspend fun TranIntentMarket.fromAmount(markets: Array<TranIntentMarket>, idx: Int): Amount {
            return when (this) {
                is TranIntentMarketCompleted -> fromAmount
                is TranIntentMarketPartiallyCompleted -> fromAmount
                is TranIntentMarketPredicted -> predictedFromAmount(markets, idx)
            }
        }

        private suspend fun TranIntentMarket.targetAmount(markets: Array<TranIntentMarket>, idx: Int): Amount {
            return when (this) {
                is TranIntentMarketCompleted -> targetAmount
                is TranIntentMarketPartiallyCompleted -> predictedTargetAmount()
                is TranIntentMarketPredicted -> predictedTargetAmount(markets, idx)
            }
        }

        /**
         * @throws OrderBookEmptyException
         */
        private fun getInstantOrderTargetAmount(
            orderType: OrderType,
            fromAmount: Amount,
            takerFeeMultiplier: BigDecimal,
            orderBook: OrderBookAbstract
        ): Amount {
            var unusedFromAmount: Amount = fromAmount
            var toAmount = BigDecimal.ZERO

            if (orderType == OrderType.Buy) {
                if (orderBook.asks.length() == 0) throw OrderBookEmptyException(SubBookType.Sell)

                for ((basePrice, quoteAmount) in orderBook.asks) {
                    val availableFromAmount = buyBaseAmount(quoteAmount, basePrice)

                    if (unusedFromAmount <= availableFromAmount) {
                        toAmount += buyQuoteAmount(calcQuoteAmount(unusedFromAmount, basePrice), takerFeeMultiplier)
                        break
                    } else {
                        unusedFromAmount -= buyBaseAmount(quoteAmount, basePrice)
                        toAmount += buyQuoteAmount(quoteAmount, takerFeeMultiplier)
                    }
                }
            } else {
                if (orderBook.bids.length() == 0) throw OrderBookEmptyException(SubBookType.Buy)

                for ((basePrice, quoteAmount) in orderBook.bids) {
                    if (unusedFromAmount <= quoteAmount) {
                        toAmount += sellBaseAmount(unusedFromAmount, basePrice, takerFeeMultiplier)
                        break
                    } else {
                        unusedFromAmount -= sellQuoteAmount(quoteAmount)
                        toAmount += sellBaseAmount(quoteAmount, basePrice, takerFeeMultiplier)
                    }
                }
            }

            return toAmount
        }

        /**
         * @throws OrderBookEmptyException
         */
        private fun getDelayedOrderTargetAmount(
            orderType: OrderType,
            fromAmount: Amount,
            makerFeeMultiplier: BigDecimal,
            orderBook: OrderBookAbstract
        ): Amount {
            return if (orderType == OrderType.Buy) {
                if (orderBook.bids.length() == 0) throw OrderBookEmptyException(SubBookType.Sell)

                val basePrice = orderBook.bids.head()._1
                val quoteAmount = calcQuoteAmount(fromAmount, basePrice)

                buyQuoteAmount(quoteAmount, makerFeeMultiplier)
            } else {
                if (orderBook.asks.length() == 0) throw OrderBookEmptyException(SubBookType.Sell)

                val basePrice = orderBook.asks.head()._1

                sellBaseAmount(fromAmount, basePrice, makerFeeMultiplier)
            }
        }
    }
}

sealed class TranIntentMarket(
    open val market: Market,
    open val orderSpeed: OrderSpeed,
    open val fromCurrencyType: CurrencyType
) {
    val fromCurrency: Currency by lazy(LazyThreadSafetyMode.NONE) { market.currency(fromCurrencyType) }
    val targetCurrency: Currency by lazy(LazyThreadSafetyMode.NONE) { market.currency(fromCurrencyType.inverse()) }
    val orderType: OrderType by lazy(LazyThreadSafetyMode.NONE) { market.orderType(fromCurrencyType.inverse()) }
}

data class TranIntentMarketCompleted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType,
    val trades: Array<BareTrade>
) : TranIntentMarket(market, orderSpeed, fromCurrencyType) {
    val fromAmount: Amount by lazy(LazyThreadSafetyMode.NONE) {
        var amount = BigDecimal.ZERO
        for (trade in trades) {
            amount += if (fromCurrencyType == CurrencyType.Base) {
                buyBaseAmount(trade.quoteAmount, trade.price)
            } else {
                sellQuoteAmount(trade.quoteAmount)
            }
        }
        amount
    }

    val targetAmount: Amount by lazy(LazyThreadSafetyMode.NONE) {
        var amount = BigDecimal.ZERO
        for (trade in trades) {
            amount += if (fromCurrencyType == CurrencyType.Base) {
                buyQuoteAmount(trade.quoteAmount, trade.feeMultiplier)
            } else {
                sellBaseAmount(trade.quoteAmount, trade.price, trade.feeMultiplier)
            }
        }
        amount
    }
}

data class TranIntentMarketPartiallyCompleted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType,
    val fromAmount: Amount
) : TranIntentMarket(market, orderSpeed, fromCurrencyType)

data class TranIntentMarketPredicted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType
) : TranIntentMarket(market, orderSpeed, fromCurrencyType)