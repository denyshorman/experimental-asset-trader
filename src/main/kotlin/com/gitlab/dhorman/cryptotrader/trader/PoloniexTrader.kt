package com.gitlab.dhorman.cryptotrader.trader

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonView
import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.UnfilledMarketsDao
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import io.vavr.Tuple2
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.collection.Queue
import io.vavr.collection.Vector
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.reactor.flux
import kotlinx.coroutines.reactor.mono
import mu.KotlinLogging
import org.springframework.data.r2dbc.function.TransactionalDatabaseClient
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.io.IOException
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.UnknownHostException
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.Comparator

//TODO: Implement handling for cancel events
//TODO: Add Poloniex events error handlers
//TODO: Add more logging logging and research async logging
@Service
class PoloniexTrader(
    private val poloniexApi: PoloniexApi,
    val data: DataStreams,
    val indicators: IndicatorStreams,
    private val transactionsDao: TransactionsDao,
    private val unfilledMarketsDao: UnfilledMarketsDao,
    private val tranDatabaseClient: TransactionalDatabaseClient
) {
    private val logger = KotlinLogging.logger {}

    @Volatile
    var primaryCurrencies: List<Currency> = list("USDT", "USDC")

    fun start(scope: CoroutineScope) = scope.launch {
        logger.info("Start trading")

        val tranIntentScope =
            CoroutineScope(Schedulers.parallel().asCoroutineDispatcher() + SupervisorJob(coroutineContext[Job]))

        resumeSleepingTransactions(tranIntentScope)

        try {
            val tickerFlow = Flux.interval(Duration.ofSeconds(60))
                .startWith(0)
                .onBackpressureDrop()

            tickerFlow.collect {
                val (startCurrency, requestedAmount) = requestBalanceForTransaction() ?: return@collect

                val bestPath =
                    selectBestPath(requestedAmount, startCurrency, requestedAmount, primaryCurrencies) ?: return@collect

                startPathTransaction(bestPath, tranIntentScope)
            }
        } finally {
            tranIntentScope.cancel()
        }
    }

    private suspend fun resumeSleepingTransactions(scope: CoroutineScope) {
        for ((id, markets) in transactionsDao.getAll()) {
            val startMarketIdx = partiallyCompletedMarketIndex(markets)!!
            val initAmount = markets.first().fromAmount(markets, 0)
            val targetAmount = markets.last().targetAmount(markets, markets.length() - 1)

            if (initAmount > targetAmount) {
                val currMarket = markets[startMarketIdx] as TranIntentMarketPartiallyCompleted
                val fromCurrency = currMarket.fromCurrency
                val fromCurrencyAmount = currMarket.fromAmount
                scope.launch {
                    var bestPath: Array<TranIntentMarket>? = null
                    while (isActive) {
                        bestPath = findNewPath(initAmount, fromCurrency, fromCurrencyAmount, primaryCurrencies)
                        if (bestPath != null) break
                        delay(60000)
                    }
                    val changedMarkets = updateMarketsWithBestPath(markets, startMarketIdx, bestPath!!)
                    val newId = UUID.randomUUID()

                    withContext(NonCancellable) {
                        tranDatabaseClient.inTransaction { dbClient ->
                            FlowScope.mono {
                                transactionsDao.add(newId, changedMarkets, startMarketIdx, dbClient)
                                transactionsDao.delete(id, dbClient)
                            }
                        }.awaitFirstOrNull()
                    }

                    TransactionIntent(newId, changedMarkets, startMarketIdx, scope).start()
                }
            } else {
                TransactionIntent(id, markets, startMarketIdx, scope).start()
            }
        }
    }

    private fun partiallyCompletedMarketIndex(markets: Array<TranIntentMarket>): Int? {
        var i = 0
        for (market in markets) {
            if (market is TranIntentMarketPartiallyCompleted) break
            i++
        }
        return if (i == markets.length()) null else i
    }

    private suspend fun selectBestPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>
    ): ExhaustivePath? {
        val allPaths = indicators.getPaths(fromCurrency, fromAmount, endCurrencies, fun(p): Boolean {
            val targetMarket = p.chain.lastOrNull() ?: return false
            return initAmount < targetMarket.toAmount
        }, Comparator { p0, p1 ->
            if (p0.id == p1.id) {
                0
            } else {
                p0.id.compareTo(p1.id) // TODO: Define comparator for other metrics
            }
        })

        return allPaths.headOption().orNull
    }

    private suspend fun startPathTransaction(bestPath: ExhaustivePath, scope: CoroutineScope) {
        val id = UUID.randomUUID()
        val markets = prepareMarketsForIntent(bestPath)
        val marketIdx = 0
        val fromCurrency = markets[marketIdx].fromCurrency
        val requestedAmount = (markets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount

        val canStartTransaction = tranDatabaseClient.inTransaction { dbClient ->
            FlowScope.mono {
                val allAmount = data.balances.awaitSingle().getOrNull(fromCurrency) ?: return@mono false
                val (_, amountInUse) = transactionsDao.balanceInUse(fromCurrency, dbClient) ?: return@mono false
                val availableAmount = allAmount - amountInUse

                if (availableAmount >= requestedAmount) {
                    transactionsDao.add(id, markets, marketIdx, dbClient)
                    true
                } else {
                    false
                }
            }
        }.awaitFirstOrNull() ?: return

        if (canStartTransaction) {
            TransactionIntent(id, markets, marketIdx, scope).start()
        }
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

    private suspend fun findNewPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>
    ): Array<TranIntentMarket>? {
        val bestPath = selectBestPath(initAmount, fromCurrency, fromAmount, endCurrencies) ?: return null
        return prepareMarketsForIntent(bestPath)
    }

    private fun updateMarketsWithBestPath(
        markets: Array<TranIntentMarket>,
        marketIdx: Int,
        bestPath: Array<TranIntentMarket>
    ): Array<TranIntentMarket> {
        return markets.dropRight(markets.length() - marketIdx).appendAll(bestPath)
    }

    private fun getInstantOrderTargetAmount(
        orderType: OrderType,
        fromAmount: Amount,
        takerFeeMultiplier: BigDecimal,
        orderBook: OrderBookAbstract
    ): Amount {
        var unusedFromAmount: Amount = fromAmount
        var toAmount = BigDecimal.ZERO

        if (orderType == OrderType.Buy) {
            if (orderBook.asks.length() == 0) return BigDecimal.ZERO

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
            if (orderBook.bids.length() == 0) return BigDecimal.ZERO

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

    private fun getDelayedOrderTargetAmount(
        orderType: OrderType,
        fromAmount: Amount,
        makerFeeMultiplier: BigDecimal,
        orderBook: OrderBookAbstract
    ): Amount {
        if (orderType == OrderType.Buy) {
            if (orderBook.bids.length() == 0) return BigDecimal.ZERO

            val basePrice = orderBook.bids.head()._1
            val quoteAmount = calcQuoteAmount(fromAmount, basePrice)

            return buyQuoteAmount(quoteAmount, makerFeeMultiplier)
        } else {
            if (orderBook.asks.length() == 0) return BigDecimal.ZERO

            val basePrice = orderBook.asks.head()._1

            return sellBaseAmount(fromAmount, basePrice, makerFeeMultiplier)
        }
    }

    private suspend fun predictedFromAmount(markets: Array<TranIntentMarket>, idx: Int): Amount {
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

    private suspend fun requestBalanceForTransaction(): Tuple2<Currency, Amount>? {
        // TODO: Review requestBalanceForTransaction algorithm
        val usedBalances = transactionsDao.balancesInUse(primaryCurrencies)
        val allBalances = data.balances.awaitSingle()

        val availableBalances = usedBalances.toVavrStream()
            .map { (currency, balanceInUse) ->
                allBalances.get(currency).map { all -> tuple(currency, all - balanceInUse) }
            }
            .filter { it.isDefined }
            .map { it.get() }
            .filter { it._2 > BigDecimal(2) }
            .firstOrNull()

        return availableBalances?.run {
            if (_2 > BigDecimal(5)) {
                tuple(_1, BigDecimal(5))
            } else {
                this
            }
        }
    }

    inner class TransactionIntent(
        val id: UUID,
        private val markets: Array<TranIntentMarket>,
        private val marketIdx: Int,
        private val TranIntentScope: CoroutineScope
    ) {
        fun start(): Job = TranIntentScope.launch {
            val currentMarket = markets[marketIdx] as TranIntentMarketPartiallyCompleted
            val orderBookFlow = data.getOrderBookFlowBy(currentMarket.market)
            val feeFlow = data.fee
            val newMarketIdx = marketIdx + 1
            val modifiedMarkets = tranDatabaseClient.inTransaction { dbClient ->
                FlowScope.mono {
                    // TODO: SET TRANSACTION ISOLATION LEVEL XXX

                    val unfilledMarkets =
                        unfilledMarketsDao.get(markets[0].fromCurrency, currentMarket.fromCurrency, dbClient)

                    val modifiedMarkets = mergeMarkets(markets, unfilledMarkets)

                    if (unfilledMarkets.length() != 0) {
                        transactionsDao.update(id, modifiedMarkets, marketIdx, dbClient)
                        unfilledMarketsDao.remove(markets[0].fromCurrency, currentMarket.fromCurrency, dbClient)
                    }

                    modifiedMarkets
                }
            }.awaitFirst() // TODO: Retry if transaction failed

            when (currentMarket.orderSpeed) {
                OrderSpeed.Instant -> run {
                    val trades = tradeInstantly(
                        currentMarket.market,
                        currentMarket.fromCurrency,
                        currentMarket.fromAmount,
                        orderBookFlow,
                        feeFlow
                    )

                    val (unfilledTradeMarkets, committedMarkets) = splitMarkets(modifiedMarkets, marketIdx, trades)

                    val unfilledFromAmount = unfilledTradeMarkets[marketIdx].fromAmount(unfilledTradeMarkets, marketIdx)

                    if (unfilledFromAmount.compareTo(BigDecimal.ZERO) != 0) {
                        unfilledMarketsDao.add(
                            unfilledTradeMarkets[0].fromCurrency,
                            unfilledTradeMarkets[0].fromAmount(unfilledTradeMarkets, 0),
                            unfilledTradeMarkets[marketIdx].fromCurrency,
                            unfilledFromAmount
                        )
                    }

                    if (newMarketIdx != modifiedMarkets.length()) {
                        transactionsDao.update(id, committedMarkets, newMarketIdx)
                        TransactionIntent(id, committedMarkets, newMarketIdx, TranIntentScope).start()
                    } else {
                        transactionsDao.addCompleted(id, committedMarkets)
                        transactionsDao.delete(id)
                    }
                }
                OrderSpeed.Delayed -> run {
                    var updatedMarkets = modifiedMarkets
                    val updatedMarketsChannel = ConflatedBroadcastChannel(updatedMarkets)
                    val profitMonitoringJob = startProfitMonitoring(updatedMarketsChannel)
                    profitMonitoringJob.start()

                    try {
                        tradeDelayed(
                            currentMarket.orderType,
                            currentMarket.market,
                            currentMarket.fromAmount,
                            orderBookFlow
                        )
                            .buffer(Duration.ofSeconds(5))
                            .map { trades ->
                                tuple(
                                    trades.map { it.orderId }.toVavrList(),
                                    Array.ofAll(trades.map { it as BareTrade })
                                )
                            }
                            .collect { (orderIds, trades) ->
                                val marketSplit = splitMarkets(updatedMarkets, marketIdx, trades)
                                updatedMarkets = marketSplit._1
                                updatedMarketsChannel.send(updatedMarkets)
                                val committedMarkets = marketSplit._2

                                if (newMarketIdx != modifiedMarkets.length()) {
                                    val newId = UUID.randomUUID()
                                    tranDatabaseClient.inTransaction { dbClient ->
                                        FlowScope.mono {
                                            transactionsDao.removeOrderIds(id, orderIds, dbClient)
                                            transactionsDao.update(id, updatedMarkets, marketIdx, dbClient)
                                            transactionsDao.add(newId, committedMarkets, newMarketIdx, dbClient)
                                        }
                                    }
                                    TransactionIntent(newId, committedMarkets, newMarketIdx, TranIntentScope).start()
                                } else {
                                    transactionsDao.addCompleted(id, committedMarkets)
                                }
                            }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        if (logger.isDebugEnabled) logger.warn(e.message, e)
                    } finally {
                        // Handle unfilled amount
                        profitMonitoringJob.cancelAndJoin()

                        val initMarket = updatedMarkets[0]
                        val currMarket = updatedMarkets[marketIdx]

                        if (currMarket.fromAmount(updatedMarkets, marketIdx).compareTo(BigDecimal.ZERO) != 0) {
                            tranDatabaseClient.inTransaction { dbClient ->
                                FlowScope.mono {
                                    transactionsDao.delete(id, dbClient)
                                    unfilledMarketsDao.add(
                                        initMarket.fromCurrency,
                                        initMarket.fromAmount(updatedMarkets, 0),
                                        currMarket.fromCurrency,
                                        currMarket.fromAmount(updatedMarkets, marketIdx),
                                        dbClient
                                    )
                                }
                            }.awaitFirstOrNull()
                        } else {
                            transactionsDao.delete(id)
                        }
                    }
                }
            }
        }

        private fun mergeMarkets(
            currentMarkets: Array<TranIntentMarket>,
            unfilledMarkets: List<Tuple2<Amount, Amount>>?
        ): Array<TranIntentMarket> {
            if (unfilledMarkets == null || unfilledMarkets.length() == 0) return currentMarkets

            var newMarkets = currentMarkets

            for (amounts in unfilledMarkets) {
                newMarkets = mergeMarkets(newMarkets, amounts._1, amounts._2)
            }

            return newMarkets
        }

        private fun mergeMarkets(
            currentMarkets: Array<TranIntentMarket>,
            initCurrencyAmount: Amount,
            currentCurrencyAmount: Amount
        ): Array<TranIntentMarket> {
            var updatedMarkets = currentMarkets
            var deltaAmount = currentCurrencyAmount

            var i = partiallyCompletedMarketIndex(currentMarkets)!! - 1

            while (i >= 0 && deltaAmount.compareTo(BigDecimal.ZERO) != 0) {
                val initAmountDefined = i == 0 && initCurrencyAmount.compareTo(BigDecimal.ZERO) != 0
                val oldMarket = updatedMarkets[i] as TranIntentMarketCompleted
                val oldTrade = oldMarket.trades[0]
                val newTrade = run {
                    var newQuoteAmount = if (oldMarket.fromCurrencyType == CurrencyType.Base) {
                        val oldTradeTargetAmount = buyQuoteAmount(oldTrade.quoteAmount, oldTrade.feeMultiplier)
                        val newTradeTargetAmount = oldTradeTargetAmount + deltaAmount
                        var newTradeQuoteAmount =
                            newTradeTargetAmount.divide(oldTrade.feeMultiplier, 8, RoundingMode.UP)
                        if (buyQuoteAmount(
                                newTradeQuoteAmount,
                                oldTrade.feeMultiplier
                            ).compareTo(newTradeTargetAmount) != 0
                        ) {
                            newTradeQuoteAmount =
                                newTradeTargetAmount.divide(oldTrade.feeMultiplier, 8, RoundingMode.DOWN)
                        }
                        newTradeQuoteAmount
                    } else {
                        val oldTradeTargetAmount =
                            sellBaseAmount(oldTrade.quoteAmount, oldTrade.price, oldTrade.feeMultiplier)
                        val newTradeTargetAmount = oldTradeTargetAmount + deltaAmount
                        calcQuoteAmountSellTrade(newTradeTargetAmount, oldTrade.price, oldTrade.feeMultiplier)
                    }

                    val newPrice = if (initAmountDefined) {
                        if (oldMarket.fromCurrencyType == CurrencyType.Base) {
                            val oldFromAmount = buyBaseAmount(oldTrade.quoteAmount, oldTrade.price)
                            val newFromAmount = buyBaseAmount(newQuoteAmount, oldTrade.price)
                            val delta = newFromAmount - oldFromAmount

                            when (delta.compareTo(initCurrencyAmount)) {
                                0, 1 -> oldTrade.price
                                else -> run {
                                    val fromAmountDelta = initCurrencyAmount - delta
                                    val newFromAmount2 = newFromAmount + fromAmountDelta
                                    var newPrice = newFromAmount2.divide(newQuoteAmount, 8, RoundingMode.DOWN)
                                    if (buyBaseAmount(newQuoteAmount, newPrice).compareTo(newFromAmount2) != 0) {
                                        newPrice = newFromAmount2.divide(newQuoteAmount, 8, RoundingMode.UP)
                                    }
                                    newPrice
                                }
                            }
                        } else {
                            val oldFromAmount = sellQuoteAmount(oldTrade.quoteAmount)
                            val newFromAmount = sellQuoteAmount(newQuoteAmount)
                            val delta = newFromAmount - oldFromAmount

                            when (delta.compareTo(initCurrencyAmount)) {
                                0, 1 -> oldTrade.price
                                else -> run {
                                    val fromAmountDelta = initCurrencyAmount - delta
                                    newQuoteAmount = newFromAmount + fromAmountDelta
                                    val oldTradeTargetAmount =
                                        sellBaseAmount(oldTrade.quoteAmount, oldTrade.price, oldTrade.feeMultiplier)
                                    val newTradeTargetAmount = oldTradeTargetAmount + deltaAmount
                                    calcQuoteAmountSellTrade(
                                        newTradeTargetAmount,
                                        newQuoteAmount,
                                        oldTrade.feeMultiplier
                                    )
                                }
                            }
                        }
                    } else {
                        oldTrade.price
                    }

                    BareTrade(newQuoteAmount, newPrice, oldTrade.feeMultiplier)
                }
                val newTrades = oldMarket.trades.update(0, newTrade)
                val newMarket = TranIntentMarketCompleted(
                    oldMarket.market,
                    oldMarket.orderSpeed,
                    oldMarket.fromCurrencyType,
                    newTrades
                )
                val oldMarkets = updatedMarkets
                updatedMarkets = updatedMarkets.update(i, newMarket)
                deltaAmount =
                    (updatedMarkets[i] as TranIntentMarketCompleted).fromAmount - (oldMarkets[i] as TranIntentMarketCompleted).fromAmount
                i--
            }

            return updatedMarkets
        }

        private suspend fun tradeInstantly(
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

                val firstSimulatedTrade = simulatedTrades._2.headOption().orNull

                if (firstSimulatedTrade == null) {
                    delay(2000)
                    continue
                }

                val expectQuoteAmount = if (orderType == OrderType.Buy) {
                    calcQuoteAmount(firstSimulatedTrade.quoteAmount, firstSimulatedTrade.price)
                } else {
                    firstSimulatedTrade.quoteAmount
                }

                val transaction: BuySell
                try {
                    transaction = if (orderType == OrderType.Buy) {
                        poloniexApi.buy(
                            market,
                            firstSimulatedTrade.price,
                            expectQuoteAmount,
                            BuyOrderType.FillOrKill
                        )
                    } else {
                        poloniexApi.sell(
                            market,
                            firstSimulatedTrade.price,
                            expectQuoteAmount,
                            BuyOrderType.FillOrKill
                        )
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: TransactionFailedException) {
                    delay(500)
                    continue
                } catch (e: NotEnoughCryptoException) {
                    logger.error(e.originalMsg)
                    break
                } catch (e: TotalMustBeAtLeastException) {
                    if (logger.isDebugEnabled) logger.warn(e.originalMsg)
                    break
                } catch (e: RateMustBeLessThanException) {
                    logger.warn(e.originalMsg)
                    break
                } catch (e: MaxOrdersExceededException) {
                    logger.warn(e.originalMsg)
                    delay(1500)
                    continue
                } catch (e: UnknownHostException) {
                    delay(2000)
                    continue
                } catch (e: IOException) {
                    delay(2000)
                    continue
                } catch (e: Exception) {
                    // TODO: Correctly handle FillOrKill exception
                    delay(2000)
                    if (logger.isDebugEnabled) logger.error(e.message, e)
                    continue
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

        private fun tradeDelayed(
            orderType: OrderType,
            market: Market,
            fromCurrencyAmount: Amount,
            orderBook: Flux<OrderBookAbstract>
        ): Flux<BareTradeWithId> = FlowScope.flux {
            val tradesChannel: ProducerScope<BareTradeWithId> = this
            val unfilledAmountChannel = ConflatedBroadcastChannel(fromCurrencyAmount)
            val latestOrderIdsChannel = ConflatedBroadcastChannel(Queue.empty<Long>())

            // Fetch uncaught trades
            val predefinedOrderIds = transactionsDao.getOrderIds(id)
            if (predefinedOrderIds.isNotEmpty()) {
                latestOrderIdsChannel.send(Queue.ofAll(predefinedOrderIds).reverse())

                while (isActive) {
                    try {
                        var unfilledAmount = fromCurrencyAmount

                        // TODO: Specify time period
                        poloniexApi.tradeHistory(market).getOrNull(market)?.run {
                            for (trade in this) {
                                if (predefinedOrderIds.contains(trade.orderId)) {
                                    unfilledAmount -= if (orderType == OrderType.Buy) {
                                        buyBaseAmount(trade.amount, trade.price)
                                    } else {
                                        sellQuoteAmount(trade.amount)
                                    }

                                    val trade0 =
                                        BareTradeWithId(trade.orderId, trade.amount, trade.price, trade.feeMultiplier)

                                    tradesChannel.send(trade0)
                                }
                            }
                        }

                        unfilledAmountChannel.send(unfilledAmount)

                        if (unfilledAmount.compareTo(BigDecimal.ZERO) == 0) {
                            tradesChannel.close()
                            return@flux
                        }

                        break
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        if (logger.isDebugEnabled) logger.warn(e.message, e)
                        delay(1000)
                    }
                }
            }

            // Trades monitoring
            launch {
                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Starting trades consumption...")

                var unfilledAmount = unfilledAmountChannel.value

                poloniexApi.accountNotificationStream.collect { trade ->
                    if (trade !is TradeNotification) return@collect

                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trade received: $trade")

                    val orderIds = latestOrderIdsChannel.value

                    for (orderId in orderIds.reverseIterator()) {
                        if (orderId != trade.orderId) continue

                        if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trade matched. Sending trade to the client..")

                        unfilledAmount -= if (orderType == OrderType.Buy) {
                            buyBaseAmount(trade.amount, trade.price)
                        } else {
                            sellQuoteAmount(trade.amount)
                        }

                        unfilledAmountChannel.send(unfilledAmount)

                        tradesChannel.send(
                            BareTradeWithId(trade.orderId, trade.amount, trade.price, trade.feeMultiplier)
                        )

                        break
                    }
                }
            }

            // Place - Move order loop
            launch placeMoveOrderLoop@{
                var lastOrderId: Long? = latestOrderIdsChannel.value.lastOrNull()
                var prevPrice: Price? = null

                suspend fun handlePlaceMoveOrderResult(res: Tuple2<Long, Price>) {
                    lastOrderId = res._1
                    prevPrice = res._2

                    var latestOrderIds = latestOrderIdsChannel.value
                    latestOrderIds = latestOrderIds.append(lastOrderId)
                    if (latestOrderIds.size() > 16) latestOrderIds = latestOrderIds.drop(1)
                    latestOrderIdsChannel.send(latestOrderIds)
                    transactionsDao.addOrderId(id, lastOrderId!!) // TODO: Send to db asynchronously ?
                }

                suspend fun placeAndHandleOrder() {
                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Placing new order with amount ${unfilledAmountChannel.value}...")

                    val placeOrderResult = placeOrder(market, orderBook, orderType, unfilledAmountChannel.value)

                    handlePlaceMoveOrderResult(placeOrderResult)

                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] New order placed: $lastOrderId, $prevPrice")
                }

                if (lastOrderId == null) {
                    placeAndHandleOrder()
                } else {
                    var orderStatus: OrderStatus?

                    while (true) {
                        try {
                            orderStatus = poloniexApi.orderStatus(lastOrderId!!)
                            break
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            if (logger.isDebugEnabled) logger.warn("Can't get order status for order id ${lastOrderId!!}: ${e.message}")
                            delay(1000)
                        }
                    }

                    if (orderStatus == null) {
                        placeAndHandleOrder()
                    } else {
                        prevPrice = orderStatus.price
                    }
                }

                orderBook.onBackpressureLatest().collect orderBookLabel@{ book ->
                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trying to move order $lastOrderId...")

                    val moveOrderResult =
                        moveOrder(market, book, orderType, lastOrderId!!, prevPrice!!, unfilledAmountChannel)
                            ?: return@orderBookLabel

                    handlePlaceMoveOrderResult(moveOrderResult)

                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Order $lastOrderId moved to ${moveOrderResult._1} with price ${moveOrderResult._2}")
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
            amountChannel: ConflatedBroadcastChannel<Amount>
        ): Tuple2<Long, Price>? {
            var retryCount = 0

            while (true) {
                try {
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
                            ?: throw OrderBookEmptyException(orderType)
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
                            // TODO: Implement better algorithm to handle price gaps
                        }
                    } ?: return null

                    val moveOrderResult = poloniexApi.moveOrder(
                        lastOrderId,
                        newPrice,
                        amountChannel.value,
                        BuyOrderType.PostOnly
                    )

                    return tuple(moveOrderResult.orderId, newPrice)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: TransactionFailedException) {
                    retryCount = 0
                    delay(500)
                } catch (e: InvalidOrderNumberException) {
                    throw e
                } catch (e: NotEnoughCryptoException) {
                    if (retryCount++ == 3) {
                        throw e
                    } else {
                        delay(1000)
                    }
                } catch (e: TotalMustBeAtLeastException) {
                    throw e
                } catch (e: RateMustBeLessThanException) {
                    throw e
                } catch (e: OrderBookEmptyException) {
                    retryCount = 0
                    logger.warn(e.message)
                    delay(1000)
                } catch (e: MaxOrdersExceededException) {
                    retryCount = 0
                    logger.warn(e.originalMsg)
                    delay(1500)
                } catch (e: UnknownHostException) {
                    retryCount = 0
                    delay(2000)
                } catch (e: IOException) {
                    retryCount = 0
                    delay(2000)
                } catch (e: Exception) {
                    retryCount = 0
                    delay(2000)
                    if (logger.isDebugEnabled) logger.error(e.message, e)
                }
            }
        }

        private suspend fun placeOrder(
            market: Market,
            orderBook: Flux<OrderBookAbstract>,
            orderType: OrderType,
            amountValue: BigDecimal
        ): Tuple2<Long, Price> {
            while (true) {
                try {
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

                        val anotherWorkerOnTheSameMarket =
                            bookOrders.contains(BookOrder(market, topPricePrimary, orderType))

                        if (anotherWorkerOnTheSameMarket) {
                            topPricePrimary
                        } else {
                            val newPrice = moveToOnePoint(topPricePrimary)
                            val topPriceSecondary = secondaryBook.headOption().map { it._1 }.orNull
                            if (topPriceSecondary != null && topPriceSecondary.compareTo(newPrice) == 0) {
                                topPricePrimary
                            } else {
                                newPrice
                            }
                        }
                    } ?: throw OrderBookEmptyException(orderType)

                    val result = when (orderType) {
                        OrderType.Buy -> poloniexApi.buy(market, price, amountValue, BuyOrderType.PostOnly)
                        OrderType.Sell -> poloniexApi.sell(market, price, amountValue, BuyOrderType.PostOnly)
                    }

                    return tuple(result.orderId, price)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: TransactionFailedException) {
                    delay(500)
                    continue
                } catch (e: NotEnoughCryptoException) {
                    throw e
                } catch (e: TotalMustBeAtLeastException) {
                    throw e
                } catch (e: RateMustBeLessThanException) {
                    throw e
                } catch (e: OrderBookEmptyException) {
                    logger.warn(e.message)
                    delay(1000)
                    continue
                } catch (e: MaxOrdersExceededException) {
                    logger.warn(e.originalMsg)
                    delay(1500)
                    continue
                } catch (e: UnknownHostException) {
                    delay(2000)
                    continue
                } catch (e: IOException) {
                    delay(2000)
                    continue
                } catch (e: Exception) {
                    delay(2000)
                    if (logger.isDebugEnabled) logger.error(e.message, e)
                    continue
                }
            }
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
                val startTime = Instant.now()

                while (isActive) {
                    delay(10000)
                    var updatedMarkets = updatedMarketsChannel.value

                    val initAmount = updatedMarkets.first().fromAmount(updatedMarkets, 0)
                    val targetAmount = updatedMarkets.last().targetAmount(updatedMarkets, updatedMarkets.length() - 1)

                    if (initAmount > targetAmount || Duration.between(startTime, Instant.now()).toMinutes() > 40) {
                        parentJob?.cancelAndJoin()
                        updatedMarkets = updatedMarketsChannel.value
                        val currMarket = updatedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted
                        val fromCurrency = currMarket.fromCurrency
                        val fromCurrencyAmount = currMarket.fromAmount
                        var bestPath: Array<TranIntentMarket>? = null
                        while (isActive) {
                            bestPath = findNewPath(initAmount, fromCurrency, fromCurrencyAmount, primaryCurrencies)
                            if (bestPath != null) break
                            delay(60000)
                        }
                        val changedMarkets = updateMarketsWithBestPath(updatedMarkets, marketIdx, bestPath!!)
                        transactionsDao.update(id, changedMarkets, marketIdx)
                        TransactionIntent(id, changedMarkets, marketIdx, TranIntentScope).start()
                    }
                }
            }
        }

        private fun splitMarkets(
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
                                val commitQuoteAmount =
                                    calcQuoteAmountSellTrade(targetAmount, trade.price, trade.feeMultiplier)
                                val updateQuoteAmount = trade.quoteAmount - commitQuoteAmount
                                updatedTrades.add(BareTrade(updateQuoteAmount, trade.price, trade.feeMultiplier))
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

        private fun calcQuoteAmountSellTrade(
            targetAmount: Amount,
            price: Price,
            feeMultiplier: BigDecimal
        ): BigDecimal {
            val tfd: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                targetAmount.divide(
                    feeMultiplier,
                    8,
                    RoundingMode.DOWN
                )
            }
            val tfu: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
                targetAmount.divide(
                    feeMultiplier,
                    8,
                    RoundingMode.UP
                )
            }
            val qdd: BigDecimal by lazy(LazyThreadSafetyMode.NONE) { tfd.divide(price, 8, RoundingMode.DOWN) }
            val qdu: BigDecimal by lazy(LazyThreadSafetyMode.NONE) { tfd.divide(price, 8, RoundingMode.UP) }
            val qud: BigDecimal by lazy(LazyThreadSafetyMode.NONE) { tfu.divide(price, 8, RoundingMode.DOWN) }
            val quu: BigDecimal by lazy(LazyThreadSafetyMode.NONE) { tfu.divide(price, 8, RoundingMode.UP) }

            return when {
                sellBaseAmount(qdd, price, feeMultiplier).compareTo(targetAmount) == 0 -> qdd
                sellBaseAmount(qdu, price, feeMultiplier).compareTo(targetAmount) == 0 -> qdu
                sellBaseAmount(qud, price, feeMultiplier).compareTo(targetAmount) == 0 -> qud
                sellBaseAmount(quu, price, feeMultiplier).compareTo(targetAmount) == 0 -> quu
                else -> throw Exception("Can't find quote amount that matches target price")
            }
        }
    }
}

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "tpe"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = TranIntentMarketCompleted::class, name = "c"),
    JsonSubTypes.Type(value = TranIntentMarketPartiallyCompleted::class, name = "pc"),
    JsonSubTypes.Type(value = TranIntentMarketPredicted::class, name = "p")
)
sealed class TranIntentMarket(
    open val market: Market,
    open val orderSpeed: OrderSpeed,
    open val fromCurrencyType: CurrencyType
) {
    @get:JsonView(Views.UI::class)
    val fromCurrency: Currency by lazy(LazyThreadSafetyMode.NONE) { market.currency(fromCurrencyType) }

    @get:JsonView(Views.UI::class)
    val targetCurrency: Currency by lazy(LazyThreadSafetyMode.NONE) { market.currency(fromCurrencyType.inverse()) }

    @get:JsonView(Views.UI::class)
    val orderType: OrderType by lazy(LazyThreadSafetyMode.NONE) { market.orderType(fromCurrencyType.inverse()) }
}

data class TranIntentMarketCompleted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType,
    val trades: Array<BareTrade>
) : TranIntentMarket(market, orderSpeed, fromCurrencyType) {
    @get:JsonView(Views.UI::class)
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

    @get:JsonView(Views.UI::class)
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

object Views {
    open class DB
    open class UI : DB()
}