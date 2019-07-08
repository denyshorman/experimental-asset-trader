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
import com.gitlab.dhorman.cryptotrader.util.buffer
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.collection.Queue
import io.vavr.collection.Vector
import io.vavr.collection.Map
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.reactor.mono
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Flux
import java.io.IOException
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.UnknownHostException
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.Comparator

@Service
class PoloniexTrader(
    private val poloniexApi: PoloniexApi,
    val data: DataStreams,
    val indicators: IndicatorStreams,
    private val transactionsDao: TransactionsDao,
    private val unfilledMarketsDao: UnfilledMarketsDao,
    @Qualifier("pg_tran_manager") private val tranManager: ReactiveTransactionManager
) {
    private val logger = KotlinLogging.logger {}

    @Volatile
    var primaryCurrencies: List<Currency> = list("USDT", "USDC")

    @Volatile
    var fixedAmount: Map<Currency, Amount> = hashMap(
        "USDT" to BigDecimal(90),
        "USDC" to BigDecimal(30)
    )

    fun start(scope: CoroutineScope) = scope.launch {
        logger.info("Start trading")

        val tranIntentScope = CoroutineScope(Dispatchers.Default + SupervisorJob(coroutineContext[Job]))

        resumeSleepingTransactions(tranIntentScope)

        try {
            val tickerFlow = Flux.interval(Duration.ofSeconds(30))
                .startWith(0)
                .onBackpressureDrop()

            tickerFlow.collect {
                logger.debug { "Trying to find new transaction..." }

                val (startCurrency, requestedAmount) = requestBalanceForTransaction() ?: return@collect

                logger.debug { "Requested currency $startCurrency and amount $requestedAmount for transaction" }

                val bestPath =
                    selectBestPath(requestedAmount, startCurrency, requestedAmount, primaryCurrencies) ?: return@collect

                logger.debug {
                    val startAmount = bestPath.chain.head().fromAmount
                    val endAmount = bestPath.chain.last().toAmount
                    val longPath = bestPath.longPathString()

                    "Found optimal path: $longPath, using amount $startAmount with potential profit ${endAmount - startAmount}"
                }

                startPathTransaction(bestPath, tranIntentScope)
            }
        } finally {
            logger.debug { "Trying to cancel all Poloniex transactions..." }

            tranIntentScope.cancel()
        }
    }

    private suspend fun resumeSleepingTransactions(scope: CoroutineScope) {
        logger.debug { "Trying to resume sleeping transactions..." }

        val sleepingTransactions = transactionsDao.getActive()

        for ((id, markets) in sleepingTransactions) {
            val startMarketIdx = partiallyCompletedMarketIndex(markets)!!
            val initAmount = markets.first().fromAmount(markets, 0)
            val targetAmount = markets.last().targetAmount(markets, markets.length() - 1)

            if (initAmount > targetAmount) {
                logger.debug { "Restored path $id is not profitable (${targetAmount - initAmount}). Trying to find new path..." }

                val currMarket = markets[startMarketIdx] as TranIntentMarketPartiallyCompleted
                val fromCurrency = currMarket.fromCurrency
                val fromCurrencyAmount = currMarket.fromAmount
                scope.launch {
                    var bestPath: Array<TranIntentMarket>?

                    while (true) {
                        bestPath = findNewPath(initAmount, fromCurrency, fromCurrencyAmount, primaryCurrencies)

                        if (bestPath != null) {
                            logger.debug { "Found optimal path ${bestPath.pathString()} for $id" }

                            break
                        } else {
                            logger.debug { "Optimal path not found for $id. Retrying..." }
                        }

                        delay(60000)
                    }

                    val changedMarkets = updateMarketsWithBestPath(markets, startMarketIdx, bestPath!!)
                    val newId = UUID.randomUUID()

                    withContext(NonCancellable) {
                        TransactionalOperator.create(tranManager).transactional(FlowScope.mono {
                            transactionsDao.addActive(newId, changedMarkets, startMarketIdx)
                            transactionsDao.deleteActive(id)
                        }).retry().awaitFirstOrNull()
                    }

                    TransactionIntent(newId, changedMarkets, startMarketIdx, scope).start()
                }
            } else {
                TransactionIntent(id, markets, startMarketIdx, scope).start()
            }
        }

        logger.debug { "Sleeping transactions restored: ${sleepingTransactions.size}" }
    }

    private fun partiallyCompletedMarketIndex(markets: Array<TranIntentMarket>): Int? {
        var i = 0
        for (market in markets) {
            if (market is TranIntentMarketPartiallyCompleted) break
            i++
        }
        return if (i == markets.length()) null else i
    }

    private fun Array<TranIntentMarket>.pathString(): String {
        return this.iterator()
            .map { "${it.market}${if (it.orderSpeed == OrderSpeed.Instant) "0" else "1"}" }
            .mkString("->")
    }

    private fun Array<TranIntentMarket>.id(): String {
        return this.iterator().map {
            val speed = if (it.orderSpeed == OrderSpeed.Instant) "0" else "1"
            "${it.market.baseCurrency}${it.market.quoteCurrency}$speed"
        }.mkString("")
    }

    private suspend fun selectBestPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>,
        recommendedChainCount: Int? = null
    ): ExhaustivePath? {
        val activeTransactionIds = transactionsDao.getActive().map { it._2.id() }

        val allPaths = indicators.getPaths(fromCurrency, fromAmount, endCurrencies, fun(p): Boolean {
            val targetMarket = p.chain.lastOrNull() ?: return false
            return initAmount < targetMarket.toAmount && !activeTransactionIds.contains(p.id)
        }, Comparator { p0, p1 ->
            if (p0.id == p1.id) {
                0
            } else {
                if (recommendedChainCount == null) {
                    p1.profitability.compareTo(p0.profitability)
                } else {
                    val c0 = p0.chain.length()
                    val c1 = p1.chain.length()

                    if (c0 <= recommendedChainCount && c1 <= recommendedChainCount) {
                        p1.profitability.compareTo(p0.profitability)
                    } else if (c0 <= recommendedChainCount) {
                        -1
                    } else if (c1 <= recommendedChainCount) {
                        1
                    } else {
                        val c = c0.compareTo(c1)

                        if (c == 0) {
                            p1.profitability.compareTo(p0.profitability)
                        } else {
                            c
                        }
                    }
                }
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

        val canStartTransaction = TransactionalOperator.create(tranManager, object : TransactionDefinition {
            override fun getIsolationLevel() = TransactionDefinition.ISOLATION_REPEATABLE_READ
        }).transactional(FlowScope.mono {
            val (available, onOrders) = data.balances.first().getOrNull(fromCurrency) ?: return@mono false
            val (_, amountInUse) = transactionsDao.balanceInUse(fromCurrency) ?: tuple(fromCurrency, BigDecimal.ZERO)
            val reservedAmount = onOrders - amountInUse
            val availableAmount = available + if (reservedAmount >= BigDecimal.ZERO) BigDecimal.ZERO else reservedAmount

            if (availableAmount >= requestedAmount) {
                transactionsDao.addActive(id, markets, marketIdx)
                true
            } else {
                false
            }
        }).retry().awaitFirstOrNull() ?: return

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
        endCurrencies: List<Currency>,
        recommendedChainCount: Int? = null
    ): Array<TranIntentMarket>? {
        val bestPath =
            selectBestPath(initAmount, fromCurrency, fromAmount, endCurrencies, recommendedChainCount) ?: return null
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
        val fee = data.fee.first()
        val orderBook = data.getOrderBookFlowBy(market).first()
        val fromAmount = predictedFromAmount(markets, idx)

        return if (orderSpeed == OrderSpeed.Instant) {
            getInstantOrderTargetAmount(orderType, fromAmount, fee.taker, orderBook)
        } else {
            getDelayedOrderTargetAmount(orderType, fromAmount, fee.maker, orderBook)
        }
    }

    private suspend fun TranIntentMarketPartiallyCompleted.predictedTargetAmount(): Amount {
        val fee = data.fee.first()
        val orderBook = data.getOrderBookFlowBy(market).first()

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
            .groupBy({ it._1 }, { it._2 })
            .mapValues { it.value.reduce { a, b -> a + b } }

        val allBalances = data.balances.first()

        val availableBalance = allBalances.toVavrStream()
            .filter { primaryCurrencies.contains(it._1) }
            .map { currencyAvailableAndOnOrders ->
                val (currency, availableAndOnOrders) = currencyAvailableAndOnOrders
                val (available, onOrders) = availableAndOnOrders
                val balanceInUse = usedBalances.getOrDefault(currency, BigDecimal.ZERO)
                val reservedAmount = onOrders - balanceInUse
                var availableAmount = available - fixedAmount.getOrElse(
                    currency,
                    BigDecimal.ZERO
                ) + if (reservedAmount >= BigDecimal.ZERO) BigDecimal.ZERO else reservedAmount
                if (availableAmount < BigDecimal.ZERO) availableAmount = BigDecimal.ZERO
                tuple(currency, availableAmount)
            }
            .filter { it._2 > BigDecimal(2) }
            .firstOrNull() ?: return null

        val (currency, amount) = availableBalance

        return if (amount > BigDecimal(10)) {
            tuple(currency, BigDecimal(10))
        } else {
            availableBalance
        }
    }

    inner class TransactionIntent(
        val id: UUID,
        private val markets: Array<TranIntentMarket>,
        private val marketIdx: Int,
        private val TranIntentScope: CoroutineScope
    ) {
        fun start(): Job = TranIntentScope.launch {
            logger.debug { "Starting path traversal for ($marketIdx) ${markets.pathString()}" }

            val currentMarket = markets[marketIdx] as TranIntentMarketPartiallyCompleted
            val orderBookFlow = data.getOrderBookFlowBy(currentMarket.market)
            val feeFlow = data.fee
            val newMarketIdx = marketIdx + 1
            val modifiedMarkets = withContext(NonCancellable) {
                TransactionalOperator.create(tranManager, object : TransactionDefinition {
                    override fun getIsolationLevel() = TransactionDefinition.ISOLATION_REPEATABLE_READ
                }).transactional(FlowScope.mono {
                    val unfilledMarkets =
                        unfilledMarketsDao.get(markets[0].fromCurrency, currentMarket.fromCurrency)

                    val modifiedMarkets = mergeMarkets(markets, unfilledMarkets)

                    if (unfilledMarkets.length() != 0) {
                        transactionsDao.updateActive(id, modifiedMarkets, marketIdx)
                        unfilledMarketsDao.remove(markets[0].fromCurrency, currentMarket.fromCurrency)
                    }

                    modifiedMarkets
                }).retry().awaitFirst()
            }

            if (currentMarket.orderSpeed == OrderSpeed.Instant) {
                val fromAmount = (modifiedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount

                val trades = tradeInstantly(
                    currentMarket.market,
                    currentMarket.fromCurrency,
                    fromAmount,
                    orderBookFlow,
                    feeFlow
                )

                logger.debug { "Instant ${currentMarket.orderType} has been completed. Trades: $trades" }

                withContext(NonCancellable) {
                    if (trades.length() == 0) {
                        if (marketIdx == 0) transactionsDao.deleteActive(id)
                        return@withContext
                    }

                    val (unfilledTradeMarkets, committedMarkets) = splitMarkets(modifiedMarkets, marketIdx, trades)

                    val unfilledFromAmount =
                        unfilledTradeMarkets[marketIdx].fromAmount(unfilledTradeMarkets, marketIdx)

                    if (unfilledFromAmount.compareTo(BigDecimal.ZERO) != 0 && marketIdx != 0) {
                        val fromCurrencyInit = unfilledTradeMarkets[0].fromCurrency
                        val fromCurrencyInitAmount = unfilledTradeMarkets[0].fromAmount(unfilledTradeMarkets, 0)
                        val fromCurrencyCurrent = unfilledTradeMarkets[marketIdx].fromCurrency

                        val primaryCurrencyUnfilled = primaryCurrencies.contains(fromCurrencyCurrent)
                                && primaryCurrencies.contains(fromCurrencyInit)
                                && fromCurrencyInitAmount <= unfilledFromAmount

                        if (!primaryCurrencyUnfilled) {
                            unfilledMarketsDao.add(
                                fromCurrencyInit,
                                fromCurrencyInitAmount,
                                fromCurrencyCurrent,
                                unfilledFromAmount
                            )
                        }
                    }

                    if (newMarketIdx != modifiedMarkets.length()) {
                        transactionsDao.updateActive(id, committedMarkets, newMarketIdx)
                        TransactionIntent(id, committedMarkets, newMarketIdx, TranIntentScope).start()
                    } else {
                        TransactionalOperator.create(tranManager).transactional(FlowScope.mono {
                            transactionsDao.addCompleted(id, committedMarkets)
                            transactionsDao.deleteActive(id)
                        }).retry().awaitFirstOrNull()
                    }
                }
            } else {
                var updatedMarkets = modifiedMarkets
                val updatedMarketsRef = AtomicReference(updatedMarkets)
                val cancelByProfitMonitoringJob = AtomicBoolean(false)
                val profitMonitoringJob = startProfitMonitoring(updatedMarketsRef, cancelByProfitMonitoringJob)

                try {
                    while (isActive) {
                        try {
                            var fromAmount =
                                (updatedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount

                            tradeDelayed(
                                currentMarket.orderType,
                                currentMarket.market,
                                fromAmount,
                                orderBookFlow
                            )
                                .buffer(Duration.ofSeconds(5))
                                .map { it.flatten() }
                                .collect { tradesAndOrderIds ->
                                    withContext(NonCancellable) {
                                        val trades = Array.ofAll(tradesAndOrderIds.map { it._2 })
                                        val marketSplit = splitMarkets(updatedMarkets, marketIdx, trades)
                                        updatedMarkets = marketSplit._1
                                        val committedMarkets = marketSplit._2

                                        fromAmount =
                                            (updatedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount

                                        logger.debug { "[${currentMarket.market}, ${currentMarket.orderType}] Received delayed trades: $trades" }

                                        if (fromAmount.compareTo(BigDecimal.ZERO) == 0) {
                                            profitMonitoringJob.cancelAndJoin()
                                            cancelByProfitMonitoringJob.set(false)
                                        } else {
                                            updatedMarketsRef.set(updatedMarkets)
                                        }

                                        if (newMarketIdx != modifiedMarkets.length()) {
                                            val newId = UUID.randomUUID()
                                            val orderIds = tradesAndOrderIds.map { it._1 }.toVavrList()

                                            TransactionalOperator.create(tranManager).transactional(FlowScope.mono {
                                                if (fromAmount.compareTo(BigDecimal.ZERO) != 0) {
                                                    transactionsDao.removeOrderIds(id, orderIds)
                                                    transactionsDao.updateActive(id, updatedMarkets, marketIdx)
                                                }

                                                transactionsDao.addActive(newId, committedMarkets, newMarketIdx)
                                            }).retry().awaitFirstOrNull()

                                            TransactionIntent(
                                                newId,
                                                committedMarkets,
                                                newMarketIdx,
                                                TranIntentScope
                                            ).start()
                                        } else {
                                            transactionsDao.addCompleted(id, committedMarkets)
                                        }
                                    }
                                }

                            break
                        } catch (e: DisconnectedException) {
                            poloniexApi.connection.filter { it }.first()
                        }
                    }
                } catch (e: CancellationException) {
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn(e.message)
                } finally {
                    val isCancelled = !isActive

                    withContext(NonCancellable) {
                        // Handle unfilled amount
                        if (cancelByProfitMonitoringJob.get()) return@withContext

                        profitMonitoringJob.cancel()

                        val initMarket = updatedMarkets[0]
                        val currMarket = updatedMarkets[marketIdx]
                        val fromAmount = currMarket.fromAmount(updatedMarkets, marketIdx)

                        if (marketIdx != 0 && fromAmount.compareTo(BigDecimal.ZERO) != 0) {
                            if (!isCancelled) {
                                val fromCurrencyInit = initMarket.fromCurrency
                                val fromCurrencyInitAmount = initMarket.fromAmount(updatedMarkets, 0)
                                val fromCurrencyCurrent = currMarket.fromCurrency
                                val fromCurrencyCurrentAmount = currMarket.fromAmount(updatedMarkets, marketIdx)

                                val primaryCurrencyUnfilled = primaryCurrencies.contains(fromCurrencyCurrent)
                                        && primaryCurrencies.contains(fromCurrencyInit)
                                        && fromCurrencyInitAmount <= fromCurrencyCurrentAmount

                                TransactionalOperator.create(tranManager).transactional(FlowScope.mono {
                                    transactionsDao.deleteActive(id)
                                    if (!primaryCurrencyUnfilled) {
                                        unfilledMarketsDao.add(
                                            fromCurrencyInit,
                                            fromCurrencyInitAmount,
                                            fromCurrencyCurrent,
                                            fromCurrencyCurrentAmount
                                        )
                                    }
                                }).retry().awaitFirstOrNull()
                            }
                        } else {
                            transactionsDao.deleteActive(id)
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
            val currMarketIdx = partiallyCompletedMarketIndex(currentMarkets)!!
            val prevMarketIdx = currMarketIdx - 1

            if (prevMarketIdx >= 0) {
                // 1. Add amount to trades of init market

                if (initCurrencyAmount.compareTo(BigDecimal.ZERO) != 0) {
                    val initMarket = updatedMarkets[0] as TranIntentMarketCompleted
                    val newTrade = BareTrade(currentCurrencyAmount, BigDecimal.ONE, BigDecimal.ZERO)
                    val newTrades = initMarket.trades.append(newTrade)
                    val newMarket = TranIntentMarketCompleted(
                        initMarket.market,
                        initMarket.orderSpeed,
                        initMarket.fromCurrencyType,
                        newTrades
                    )
                    updatedMarkets = updatedMarkets.update(0, newMarket)
                }


                // 2. Add amount to trades of previous market

                val oldMarket = updatedMarkets[prevMarketIdx] as TranIntentMarketCompleted
                val newTrade = if (oldMarket.orderType == OrderType.Buy) {
                    BareTrade(currentCurrencyAmount, BigDecimal.ZERO, BigDecimal.ONE)
                } else {
                    BareTrade(currentCurrencyAmount, BigDecimal.ZERO, BigDecimal.ZERO)
                }
                val newTrades = oldMarket.trades.append(newTrade)
                val newMarket = TranIntentMarketCompleted(
                    oldMarket.market,
                    oldMarket.orderSpeed,
                    oldMarket.fromCurrencyType,
                    newTrades
                )
                updatedMarkets = updatedMarkets.update(prevMarketIdx, newMarket)
            }

            // 3. Update current market

            val oldCurrentMarket = updatedMarkets[currMarketIdx] as TranIntentMarketPartiallyCompleted
            val newCurrentMarket = TranIntentMarketPartiallyCompleted(
                oldCurrentMarket.market,
                oldCurrentMarket.orderSpeed,
                oldCurrentMarket.fromCurrencyType,
                if (prevMarketIdx >= 0)
                    (updatedMarkets[prevMarketIdx] as TranIntentMarketCompleted).targetAmount
                else
                    oldCurrentMarket.fromAmount + initCurrencyAmount + currentCurrencyAmount
            )
            updatedMarkets = updatedMarkets.update(currMarketIdx, newCurrentMarket)

            return updatedMarkets
        }

        private suspend fun tradeInstantly(
            market: Market,
            fromCurrency: Currency,
            fromCurrencyAmount: Amount,
            orderBookFlow: Flow<OrderBookAbstract>,
            feeFlow: Flow<FeeMultiplier>
        ): Array<BareTrade> {
            val trades = LinkedList<BareTrade>()
            val feeMultiplier = feeFlow.first() // TODO: Remove when Poloniex will fix the bug with fee
            var unfilledAmount = fromCurrencyAmount

            val orderType = if (market.baseCurrency == fromCurrency) {
                OrderType.Buy
            } else {
                OrderType.Sell
            }

            try {
                var retryCount = 0

                while (unfilledAmount.compareTo(BigDecimal.ZERO) != 0) {
                    val simulatedTrades = simulateInstantTrades(unfilledAmount, orderType, orderBookFlow, feeFlow)

                    val firstSimulatedTrade = simulatedTrades._2.headOption().orNull

                    if (firstSimulatedTrade == null) {
                        delay(2000)
                        continue
                    }

                    val expectQuoteAmount = if (orderType == OrderType.Buy) {
                        calcQuoteAmount(unfilledAmount, firstSimulatedTrade.price)
                    } else {
                        firstSimulatedTrade.quoteAmount
                    }

                    if (expectQuoteAmount.compareTo(BigDecimal.ZERO) == 0) break

                    val transaction = try {
                        logger.debug { "Trying to $orderType on market $market with price ${firstSimulatedTrade.price} and amount $expectQuoteAmount" }

                        withContext(NonCancellable) {
                            if (orderType == OrderType.Buy) {
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
                        }
                    } catch (e: UnableToFillOrderException) {
                        retryCount = 0
                        logger.debug(e.originalMsg)
                        delay(100)
                        continue
                    } catch (e: TransactionFailedException) {
                        retryCount = 0
                        logger.debug(e.originalMsg)
                        delay(500)
                        continue
                    } catch (e: NotEnoughCryptoException) {
                        logger.error(e.originalMsg)

                        if (retryCount++ == 3) {
                            break
                        } else {
                            delay(1000)
                            continue
                        }
                    } catch (e: AmountMustBeAtLeastException) {
                        logger.debug(e.originalMsg)
                        break
                    } catch (e: TotalMustBeAtLeastException) {
                        logger.debug(e.originalMsg)
                        break
                    } catch (e: RateMustBeLessThanException) {
                        logger.debug(e.originalMsg)
                        break
                    } catch (e: MaxOrdersExceededException) {
                        retryCount = 0
                        logger.warn(e.originalMsg)
                        delay(1500)
                        continue
                    } catch (e: UnknownHostException) {
                        retryCount = 0
                        delay(2000)
                        continue
                    } catch (e: IOException) {
                        retryCount = 0
                        delay(2000)
                        continue
                    } catch (e: Exception) {
                        retryCount = 0
                        delay(2000)
                        if (logger.isDebugEnabled) logger.error(e.message)
                        continue
                    }

                    for (trade in transaction.trades) {
                        unfilledAmount -= if (orderType == OrderType.Buy) {
                            buyBaseAmount(trade.amount, trade.price)
                        } else {
                            sellQuoteAmount(trade.amount)
                        }

                        // TODO: Replace with transaction.feeMultiplier when Poloniex fix the bug with sell fee
                        if (transaction.feeMultiplier.compareTo(feeMultiplier.taker) != 0) {
                            logger.warn("Poloniex still has a bug with fees. Expected: ${feeMultiplier.taker}. Actual: ${transaction.feeMultiplier}")
                        }

                        trades.addLast(BareTrade(trade.amount, trade.price, feeMultiplier.taker))
                    }
                }
            } catch (e: CancellationException) {
            }

            return Array.ofAll(trades)
        }

        private fun tradeDelayed(
            orderType: OrderType,
            market: Market,
            fromCurrencyAmount: Amount,
            orderBook: Flow<OrderBookAbstract>
        ): Flow<kotlin.collections.List<Tuple2<Long, BareTrade>>> = channelFlow {
            val tradesChannel: ProducerScope<kotlin.collections.List<Tuple2<Long, BareTrade>>> = this
            val unfilledAmountChannel = ConflatedBroadcastChannel(fromCurrencyAmount)
            val latestOrderIdsRef = AtomicReference(Queue.empty<Long>())

            try {
                // Fetch uncaught trades
                val predefinedOrderIds = transactionsDao.getOrderIds(id)

                if (predefinedOrderIds.isNotEmpty()) {
                    latestOrderIdsRef.set(Queue.ofAll(predefinedOrderIds).reverse())

                    val latestOrderTs = transactionsDao.getTimestampLatestOrderId(id)!!

                    while (true) {
                        try {
                            var unfilledAmount = fromCurrencyAmount
                            val tradeList = LinkedList<Tuple2<Long, BareTrade>>()

                            poloniexApi.tradeHistory(market, latestOrderTs, limit = 75).getOrNull(market)?.run {
                                for (trade in this) {
                                    if (predefinedOrderIds.contains(trade.orderId)) {
                                        unfilledAmount -= if (orderType == OrderType.Buy) {
                                            buyBaseAmount(trade.amount, trade.price)
                                        } else {
                                            sellQuoteAmount(trade.amount)
                                        }

                                        val trade0 = BareTrade(trade.amount, trade.price, trade.feeMultiplier)
                                        tradeList.add(tuple(trade.orderId, trade0))
                                    }
                                }
                            }

                            unfilledAmountChannel.send(unfilledAmount)
                            if (tradeList.size != 0) tradesChannel.send(tradeList)

                            if (unfilledAmount.compareTo(BigDecimal.ZERO) == 0) {
                                return@channelFlow
                            }

                            break
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            if (logger.isDebugEnabled) logger.warn(e.message)
                            delay(1000)
                        }
                    }
                }

                coroutineScope {
                    val tradesJob = coroutineContext[Job]

                    // Trades monitoring
                    val tradeMonitoringJob = launch {
                        logger.debug { "[$market, $orderType] Starting trades consumption..." }

                        var unfilledAmount = unfilledAmountChannel.value

                        poloniexApi.accountNotificationStream.collect { trades ->
                            val orderIds = latestOrderIdsRef.get()
                            val tradeList = LinkedList<Tuple2<Long, BareTrade>>()

                            for (trade in trades) {
                                if (trade !is TradeNotification) continue

                                for (orderId in orderIds.reverseIterator()) {
                                    if (orderId != trade.orderId) continue

                                    unfilledAmount -= if (orderType == OrderType.Buy) {
                                        buyBaseAmount(trade.amount, trade.price)
                                    } else {
                                        sellQuoteAmount(trade.amount)
                                    }

                                    tradeList.add(
                                        tuple(
                                            orderId,
                                            BareTrade(trade.amount, trade.price, trade.feeMultiplier)
                                        )
                                    )

                                    break
                                }
                            }

                            if (tradeList.size == 0) return@collect

                            withContext(NonCancellable) {
                                unfilledAmountChannel.send(unfilledAmount)
                                tradesChannel.send(tradeList)
                            }
                        }
                    }

                    // Place - Move order loop
                    launch {
                        var lastOrderId: Long? = latestOrderIdsRef.get().lastOrNull()
                        var prevPrice: Price? = null

                        suspend fun handlePlaceMoveOrderResult(res: Tuple3<Long, Price, Amount>) {
                            lastOrderId = res._1
                            prevPrice = res._2

                            latestOrderIdsRef.getAndUpdate {
                                var ids = it.append(lastOrderId)
                                if (ids.size() > 16) ids = ids.drop(1)
                                ids
                            }

                            transactionsDao.addOrderId(id, lastOrderId!!) // TODO: Send to db asynchronously ?
                        }

                        suspend fun placeAndHandleOrder() = withContext(NonCancellable) {
                            logger.debug { "[$market, $orderType] Placing new order with amount ${unfilledAmountChannel.value.toPlainString()}..." }

                            val placeOrderResult = placeOrder(market, orderBook, orderType, unfilledAmountChannel.value)

                            handlePlaceMoveOrderResult(placeOrderResult)

                            logger.debug { "[$market, $orderType] New order placed: price ${placeOrderResult._2}, amount ${placeOrderResult._3}" }
                        }

                        try {
                            if (lastOrderId == null) {
                                placeAndHandleOrder()
                            } else {
                                var orderStatus: OrderStatus?

                                while (true) {
                                    try {
                                        logger.debug { "[$market, $orderType] Checking order status for orderId $lastOrderId" }

                                        orderStatus = poloniexApi.orderStatus(lastOrderId!!)

                                        logger.debug {
                                            if (orderStatus != null) {
                                                "[$market, $orderType] Order $lastOrderId is still active"
                                            } else {
                                                "[$market, $orderType] Order $lastOrderId not found in order book"
                                            }
                                        }

                                        break
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Exception) {
                                        logger.debug { "Can't get order status for order id $lastOrderId: ${e.message}" }
                                        delay(1000)
                                    }
                                }

                                if (orderStatus == null) {
                                    // TODO: Add more advanced handler to check for existing trades
                                    placeAndHandleOrder()
                                } else {
                                    prevPrice = orderStatus.price
                                }
                            }

                            var moveCount = 0

                            orderBook.conflate().collect { book ->
                                kotlinx.coroutines.withContext(NonCancellable) {
                                    val resp =
                                        moveOrder(
                                            market,
                                            book,
                                            orderType,
                                            lastOrderId!!,
                                            prevPrice!!,
                                            unfilledAmountChannel
                                        ) ?: return@withContext

                                    handlePlaceMoveOrderResult(resp)

                                    logger.debug { "[$market, $orderType] Moved order with price ${resp._2} and amount ${resp._3}" }

                                    if (moveCount++ > 150) {
                                        transactionsDao.deleteOldOrderIds(id, 100)
                                        moveCount = 0
                                    }
                                }
                            }
                        } finally {
                            withContext(NonCancellable) {
                                tradeMonitoringJob.cancelAndJoin()

                                if (lastOrderId != null) {
                                    while (true) {
                                        try {
                                            poloniexApi.cancelOrder(lastOrderId!!)

                                            break
                                        } catch (e: OrderCompletedOrNotExistException) {
                                            break
                                        } catch (e: UnknownHostException) {
                                            delay(2000)
                                        } catch (e: IOException) {
                                            delay(2000)
                                        } catch (e: Exception) {
                                            logger.error(e.message)
                                            delay(2000)
                                        }
                                    }

                                    logger.debug { "[$market, $orderType] Order $lastOrderId cancelled" }
                                }
                            }
                        }
                    }

                    // connection monitoring
                    launch {
                        poloniexApi.connection.collect { connected ->
                            if (!connected) throw DisconnectedException
                        }
                    }

                    // Completion monitoring
                    launch {
                        logger.debug { "[$market, $orderType] Start monitoring for all trades completion..." }

                        unfilledAmountChannel.consumeEach {
                            if (it.compareTo(BigDecimal.ZERO) == 0) {
                                logger.debug { "[$market, $orderType] All trades received" }
                                tradesJob?.cancel()
                                return@launch
                            }
                        }
                    }
                }
            } catch (e: CancellationException) {
            }
        }

        private suspend fun moveOrder(
            market: Market,
            book: OrderBookAbstract,
            orderType: OrderType,
            lastOrderId: Long,
            myPrice: Price,
            amountChannel: ConflatedBroadcastChannel<Amount>
        ): Tuple3<Long, Price, Amount>? {
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
                            val anotherWorkerOnTheSameMarket = data.orderBookOrders.first()
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

                    val quoteAmount = when (orderType) {
                        OrderType.Buy -> calcQuoteAmount(amountChannel.value, newPrice)
                        OrderType.Sell -> amountChannel.value
                    }

                    if (quoteAmount.compareTo(BigDecimal.ZERO) == 0) throw AmountIsZeroException

                    val moveOrderResult = poloniexApi.moveOrder(
                        lastOrderId,
                        newPrice,
                        quoteAmount,
                        BuyOrderType.PostOnly
                    )

                    return tuple(moveOrderResult.orderId, newPrice, quoteAmount)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: OrderCompletedOrNotExistException) {
                    throw e
                } catch (e: UnableToPlacePostOnlyOrderException) {
                    logger.debug(e.originalMsg)
                    retryCount = 0
                    delay(100)
                } catch (e: TransactionFailedException) {
                    logger.debug(e.originalMsg)
                    retryCount = 0
                    delay(500)
                } catch (e: InvalidOrderNumberException) {
                    throw e
                } catch (e: NotEnoughCryptoException) {
                    logger.debug(e.originalMsg)

                    if (retryCount++ == 3) {
                        throw e
                    } else {
                        delay(1000)
                    }
                } catch (e: AmountMustBeAtLeastException) {
                    logger.debug(e.originalMsg)
                    throw e
                } catch (e: TotalMustBeAtLeastException) {
                    logger.debug(e.originalMsg)
                    throw e
                } catch (e: RateMustBeLessThanException) {
                    logger.debug(e.originalMsg)
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
                    if (logger.isDebugEnabled) logger.error(e.message)
                }
            }
        }

        private suspend fun placeOrder(
            market: Market,
            orderBook: Flow<OrderBookAbstract>,
            orderType: OrderType,
            amountValue: BigDecimal
        ): Tuple3<Long, Price, Amount> {
            while (true) {
                try {
                    val primaryBook: SubOrderBook
                    val secondaryBook: SubOrderBook
                    val moveToOnePoint: (Price) -> Price

                    val book = orderBook.first()
                    val bookOrders = data.orderBookOrders.first()

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

                    val quoteAmount = when (orderType) {
                        OrderType.Buy -> calcQuoteAmount(amountValue, price)
                        OrderType.Sell -> amountValue
                    }

                    if (quoteAmount.compareTo(BigDecimal.ZERO) == 0) throw AmountIsZeroException

                    val result = when (orderType) {
                        OrderType.Buy -> poloniexApi.buy(market, price, quoteAmount, BuyOrderType.PostOnly)
                        OrderType.Sell -> poloniexApi.sell(market, price, quoteAmount, BuyOrderType.PostOnly)
                    }

                    return tuple(result.orderId, price, quoteAmount)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: UnableToPlacePostOnlyOrderException) {
                    logger.debug(e.originalMsg)
                    delay(100)
                    continue
                } catch (e: TransactionFailedException) {
                    logger.debug(e.originalMsg)
                    delay(500)
                    continue
                } catch (e: NotEnoughCryptoException) {
                    logger.debug(e.originalMsg)
                    throw e
                } catch (e: AmountMustBeAtLeastException) {
                    logger.debug(e.originalMsg)
                    throw e
                } catch (e: TotalMustBeAtLeastException) {
                    logger.debug(e.originalMsg)
                    throw e
                } catch (e: RateMustBeLessThanException) {
                    logger.debug(e.originalMsg)
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
                    if (logger.isDebugEnabled) logger.error(e.message)
                    continue
                }
            }
        }

        // TODO: Replace with sequence
        private suspend fun simulateInstantTrades(
            fromAmount: Amount,
            orderType: OrderType,
            orderBookFlow: Flow<OrderBookAbstract>,
            feeFlow: Flow<FeeMultiplier>
        ): Tuple2<Amount, Vector<BareTrade>> {
            val orderBook = orderBookFlow.first()
            val fee = feeFlow.first()

            var trades = Vector.empty<BareTrade>()
            var unusedFromAmount = fromAmount

            if (orderType == OrderType.Buy) {
                if (orderBook.asks.length() == 0) throw OrderBookEmptyException(SubBookType.Sell)

                for ((basePrice, quoteAmount) in orderBook.asks) {
                    val availableFromAmount = buyBaseAmount(quoteAmount, basePrice)

                    if (unusedFromAmount <= availableFromAmount) {
                        val tradeQuoteAmount = calcQuoteAmount(unusedFromAmount, basePrice)
                        trades = trades.append(BareTrade(tradeQuoteAmount, basePrice, fee.taker))
                        unusedFromAmount = BigDecimal.ZERO
                        break
                    } else {
                        unusedFromAmount -= availableFromAmount
                        trades = trades.append(BareTrade(quoteAmount, basePrice, fee.taker))
                    }
                }
            } else {
                if (orderBook.bids.length() == 0) throw OrderBookEmptyException(SubBookType.Buy)

                for ((basePrice, quoteAmount) in orderBook.bids) {
                    if (unusedFromAmount <= quoteAmount) {
                        trades = trades.append(BareTrade(unusedFromAmount, basePrice, fee.taker))
                        unusedFromAmount = BigDecimal.ZERO
                        break
                    } else {
                        unusedFromAmount -= sellQuoteAmount(quoteAmount)
                        trades = trades.append(BareTrade(quoteAmount, basePrice, fee.taker))
                    }
                }
            }

            return tuple(unusedFromAmount, trades)
        }

        private fun CoroutineScope.startProfitMonitoring(
            updatedMarketsRef: AtomicReference<Array<TranIntentMarket>>,
            cancelByProfitMonitoringJob: AtomicBoolean
        ): Job {
            val parentJob = coroutineContext[Job]

            return TranIntentScope.launch {
                val startTime = Instant.now()

                while (isActive) {
                    delay(10000)
                    var updatedMarkets = updatedMarketsRef.get()

                    val initAmount = updatedMarkets.first().fromAmount(updatedMarkets, 0)
                    val targetAmount = updatedMarkets.last().targetAmount(updatedMarkets, updatedMarkets.length() - 1)

                    val profitable = initAmount < targetAmount
                    val timeout = Duration.between(startTime, Instant.now()).toMinutes() > 40

                    if (profitable && !timeout) continue

                    logger.debug {
                        val path = updatedMarkets.pathString()

                        val reason = if (!profitable) {
                            "path is not profitable (${targetAmount - initAmount})"
                        } else {
                            "timeout has occurred"
                        }

                        "Cancelling path ($marketIdx) $path because $reason"
                    }

                    cancelByProfitMonitoringJob.set(true)
                    parentJob?.cancelAndJoin()

                    updatedMarkets = updatedMarketsRef.get()
                    val currMarket = updatedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted
                    val fromCurrency = currMarket.fromCurrency
                    val fromCurrencyAmount = currMarket.fromAmount
                    var bestPath: Array<TranIntentMarket>?

                    while (true) {
                        logger.debug { "Trying to find new path for ${updatedMarkets.pathString()}..." }

                        bestPath = findNewPath(
                            initAmount,
                            fromCurrency,
                            fromCurrencyAmount,
                            primaryCurrencies,
                            updatedMarkets.length() - marketIdx
                        )

                        if (bestPath != null) {
                            logger.debug { "New path found ${bestPath.pathString()}" }
                            break
                        } else {
                            logger.debug { "Path not found for ${updatedMarkets.pathString()}" }
                        }

                        delay(60000)
                    }

                    val changedMarkets = updateMarketsWithBestPath(updatedMarkets, marketIdx, bestPath!!)

                    withContext(NonCancellable) {
                        transactionsDao.updateActive(id, changedMarkets, marketIdx)
                    }

                    if (!isActive) break

                    TransactionIntent(id, changedMarkets, marketIdx, TranIntentScope).start()

                    break
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
                                val commitQuoteAmount: Amount
                                val targetAmountDelta: Amount

                                if (
                                    trade.price.compareTo(BigDecimal.ZERO) == 0 &&
                                    trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0
                                ) {
                                    commitQuoteAmount = targetAmount
                                    targetAmountDelta = BigDecimal.ZERO
                                } else {
                                    commitQuoteAmount =
                                        calcQuoteAmountSellTrade(targetAmount, trade.price, trade.feeMultiplier)
                                    targetAmountDelta =
                                        sellBaseAmount(commitQuoteAmount, trade.price, trade.feeMultiplier)
                                }

                                if (targetAmountDelta.compareTo(BigDecimal.ZERO) != 0) {
                                    committedTrades.add(BareTrade(targetAmountDelta, BigDecimal.ZERO, BigDecimal.ZERO))
                                }

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
            val tfd = targetAmount.divide(feeMultiplier, 8, RoundingMode.DOWN)
            val tfu = targetAmount.divide(feeMultiplier, 8, RoundingMode.UP)

            val qdd = tfd.divide(price, 8, RoundingMode.DOWN)
            val qdu = tfd.divide(price, 8, RoundingMode.UP)
            val qud = tfu.divide(price, 8, RoundingMode.DOWN)
            val quu = tfu.divide(price, 8, RoundingMode.UP)

            val a = sellBaseAmount(qdd, price, feeMultiplier)
            val b = sellBaseAmount(qdu, price, feeMultiplier)
            val c = sellBaseAmount(qud, price, feeMultiplier)
            val d = sellBaseAmount(quu, price, feeMultiplier)

            return list(tuple(qdd, a), tuple(qdu, b), tuple(qud, c), tuple(quu, d))
                .iterator()
                .filter { it._2 <= targetAmount }
                .map { it._1 }
                .max()
                .getOrElseThrow { Exception("Can't find quote amount that matches target price") }
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
                if (trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
                    BigDecimal.ZERO
                } else {
                    sellQuoteAmount(trade.quoteAmount)
                }
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
                if (trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
                    trade.quoteAmount
                } else {
                    sellBaseAmount(trade.quoteAmount, trade.price, trade.feeMultiplier)
                }
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