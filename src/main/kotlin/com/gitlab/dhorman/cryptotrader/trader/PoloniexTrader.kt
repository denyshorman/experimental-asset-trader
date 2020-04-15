package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.UnfilledMarketsDao
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPredicted
import com.gitlab.dhorman.cryptotrader.util.*
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Flux
import java.io.IOException
import java.lang.RuntimeException
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.ConnectException
import java.net.SocketException
import java.net.UnknownHostException
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.Comparator

typealias PathId = UUID

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
    private val soundSignalEnabled = System.getenv("ENABLE_SOUND_SIGNAL") != null
    private lateinit var tranIntentScope: CoroutineScope
    private lateinit var delayedTradeManager: DelayedTradeManager
    private val intentManager = IntentManager()

    @Volatile
    var primaryCurrencies: List<Currency> = list("USDT", "USDC", "USDJ", "PAX")

    @Volatile
    var fixedAmount: Map<Currency, Amount> = hashMap(
        "USDT" to BigDecimal(108),
        "USDC" to BigDecimal(0),
        "USDJ" to BigDecimal(0),
        "PAX" to BigDecimal(0)
    )

    val tranRequests = Channel<ExhaustivePath>(Channel.RENDEZVOUS)

    fun start(scope: CoroutineScope) = scope.launch(CoroutineName("PoloniexTraderStarter")) {
        logger.info("Start trading on Poloniex")

        subscribeToRequiredTopicsBeforeTrading()

        tranIntentScope = CoroutineScope(Dispatchers.Default + SupervisorJob(coroutineContext[Job]))
        delayedTradeManager = DelayedTradeManager(tranIntentScope)

        collectRoundingLeftovers()
        startMonitoringForTranRequests(tranIntentScope)
        resumeSleepingTransactions(tranIntentScope)

        try {
            val tickerFlow = Flux.interval(Duration.ofSeconds(30))
                .startWith(0)
                .onBackpressureDrop()

            tickerFlow.collect {
                logger.debug { "Trying to find new transaction..." }

                val (startCurrency, requestedAmount) = requestBalanceForTransaction() ?: run {
                    logger.debug { "Can't allocate money for new transaction" }
                    return@collect
                }

                logger.debug { "Requested currency $startCurrency and amount $requestedAmount for transaction" }

                val bestPath = selectBestPath(requestedAmount, startCurrency, requestedAmount, primaryCurrencies) ?: return@collect

                logger.debug {
                    val startAmount = bestPath.chain.head().fromAmount
                    val endAmount = bestPath.chain.last().toAmount
                    val longPath = bestPath.longPathString()

                    "Found an optimal path: $longPath, using amount $startAmount with potential profit ${endAmount - startAmount}"
                }

                startPathTransaction(bestPath, tranIntentScope)
            }
        } finally {
            logger.debug { "Trying to cancel all Poloniex transactions..." }

            tranIntentScope.cancel()
        }
    }

    private suspend fun collectRoundingLeftovers() {
        coroutineScope {
            val allBalancesAsync = async {
                val allBalances = data.balances.first()
                allBalances.removeAll(primaryCurrencies)
            }
            val activePathsAsync = async {
                transactionsDao.getActive().asSequence()
                    .map {
                        val markets = it._2
                        val idx = partiallyCompletedMarketIndex(markets)!!
                        val market = markets[idx] as TranIntentMarketPartiallyCompleted
                        tuple(market.fromCurrency, market.fromAmount)
                    }
                    .groupBy({ it._1 }, { it._2 })
                    .mapValues { it.value.fold(BigDecimal.ZERO) { a, b -> a + b } }
            }
            val unfilledAmountsAsync = async {
                unfilledMarketsDao.getAll()
                    .groupBy({ it._3 }, { it._4 })
                    .mapValues { it.value.fold(BigDecimal.ZERO) { a, b -> a + b } }
            }

            val allBalances = allBalancesAsync.await()
            val activePaths = activePathsAsync.await()
            val unfilledAmounts = unfilledAmountsAsync.await()

            val roundingLeftovers = allBalances.toVavrStream().map {
                val currency = it._1
                val allAmount = it._2._1

                val activePathAmount = activePaths.getOrDefault(currency, BigDecimal.ZERO)
                val unfilledAmount = unfilledAmounts.getOrDefault(currency, BigDecimal.ZERO)

                val delta = allAmount - (activePathAmount + unfilledAmount)

                tuple(currency, delta)
            }.filter { it._2 > BigDecimal.ZERO }.toList()

            unfilledMarketsDao.add(primaryCurrencies.first(), BigDecimal.ZERO, roundingLeftovers)

            logger.debug { "Added rounding leftovers $roundingLeftovers to unfilled amount list " }
        }
    }

    private fun CoroutineScope.startMonitoringForTranRequests(scope: CoroutineScope): Job = this.launch {
        for (tranRequest in tranRequests) {
            startPathTransaction(tranRequest, scope)
        }
    }

    private suspend fun subscribeToRequiredTopicsBeforeTrading() {
        if (logger.isTraceEnabled) logger.trace("Subscribe to topics before trading")

        coroutineScope {
            launch(start = CoroutineStart.UNDISPATCHED) {
                data.balances.first()
                if (logger.isTraceEnabled) logger.trace("Subscribed to balances topic")
            }

            launch(start = CoroutineStart.UNDISPATCHED) {
                data.openOrders.first()
                if (logger.isTraceEnabled) logger.trace("Subscribed to openOrders topic")
            }
        }

        if (logger.isTraceEnabled) logger.trace("Subscribed to all required topics before trading")
    }

    private suspend fun resumeSleepingTransactions(scope: CoroutineScope) {
        logger.debug { "Trying to resume sleeping transactions..." }

        val sleepingTransactions = transactionsDao.getActive()

        for ((id, markets) in sleepingTransactions) {
            val startMarketIdx = partiallyCompletedMarketIndex(markets)!!
            val initAmount = markets.first().fromAmount(markets, 0)
            val targetAmount = markets.last().targetAmount(markets, markets.length() - 1)

            if (initAmount > targetAmount) {
                logger.debug { "Restored path $id is not profitable (${targetAmount - initAmount}). Trying to find a new path..." }

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
                            logger.debug { "Optimal path was not found for $id (init = $initAmount, current = $fromCurrencyAmount). Retrying..." }
                        }

                        delay(60000)
                    }

                    val changedMarkets = updateMarketsWithBestPath(markets, startMarketIdx, bestPath!!)
                    val newId = UUID.randomUUID()

                    withContext(NonCancellable) {
                        TransactionalOperator.create(tranManager).transactional(mono(Dispatchers.Unconfined) {
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
        }).transactional(mono(Dispatchers.Unconfined) {
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

    suspend fun startPathTranFromUnfilledTrans(id: Long) {
        val (initCurrency, initCurrencyAmount, currentCurrency, currentCurrencyAmount) = unfilledMarketsDao.get(id)
            ?: run {
                logger.warn("Unfilled amount was not found for id $id")
                return
            }

        val f = BigDecimal("0.99910000")
        val q = currentCurrencyAmount.divide(f, 8, RoundingMode.DOWN)
        val p = if (q.compareTo(BigDecimal.ZERO) == 0) BigDecimal.ZERO else initCurrencyAmount.divide(q, 8, RoundingMode.DOWN)

        val fr = buyBaseAmount(q, p)
        val tr = buyQuoteAmount(q, f)

        val fd = initCurrencyAmount - fr
        val td = currentCurrencyAmount - tr

        val updatedMarkets = Array.of(
            TranIntentMarketCompleted(
                Market(initCurrency, currentCurrency),
                OrderSpeed.Instant,
                CurrencyType.Base,
                listOfNotNull(
                    BareTrade(q, p, f),
                    if (fd.compareTo(BigDecimal.ZERO) != 0) adjustFromAmount(fd) else null,
                    if (td.compareTo(BigDecimal.ZERO) != 0) adjustTargetAmount(td, OrderType.Buy) else null
                ).toVavrList().toArray()
            ),
            TranIntentMarketPartiallyCompleted(
                Market(initCurrency, currentCurrency),
                OrderSpeed.Instant,
                CurrencyType.Quote,
                currentCurrencyAmount
            )
        )

        val activeMarketId = updatedMarkets.length() - 1

        val bestPath = findNewPath(
            initCurrencyAmount,
            currentCurrency,
            currentCurrencyAmount,
            primaryCurrencies
        )

        if (bestPath == null) {
            logger.debug { "Path not found for ${updatedMarkets.pathString()}" }
            return
        }

        val changedMarkets = updateMarketsWithBestPath(updatedMarkets, activeMarketId, bestPath)
        val tranId = UUID.randomUUID()

        withContext(NonCancellable) {
            TransactionalOperator.create(tranManager, object : TransactionDefinition {
                override fun getIsolationLevel() = TransactionDefinition.ISOLATION_REPEATABLE_READ
            }).transactional(mono(Dispatchers.Unconfined) {
                unfilledMarketsDao.remove(id)
                transactionsDao.addActive(tranId, changedMarkets, activeMarketId)
            }).retry().awaitFirst()
        }

        TransactionIntent(tranId, changedMarkets, activeMarketId, tranIntentScope).start()
    }

    private suspend fun findNewPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>,
        recommendedChainCount: Int? = null
    ): Array<TranIntentMarket>? {
        val bestPath = selectBestPath(initAmount, fromCurrency, fromAmount, endCurrencies/*, recommendedChainCount*/) ?: return null
        return prepareMarketsForIntent(bestPath)
    }

    private suspend fun predictedFromAmount(markets: Array<TranIntentMarket>, idx: Int): Amount {
        val prevIdx = idx - 1
        return when (val prevTran = markets[prevIdx]) {
            is TranIntentMarketCompleted -> prevTran.targetAmount
            is TranIntentMarketPartiallyCompleted -> prevTran.predictedTargetAmount()
            is TranIntentMarketPredicted -> prevTran.predictedTargetAmount(markets, prevIdx)
        }
    }

    private suspend fun TranIntentMarketPredicted.predictedTargetAmount(markets: Array<TranIntentMarket>, idx: Int): Amount {
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
        logger.trace { "Requesting new balance for transaction" }

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
            .firstOrNull()

        logger.trace { "usedBalances: $usedBalances ; allBalances: $allBalances ; availableBalance: $availableBalance" }

        if (availableBalance == null) return null

        val (currency, amount) = availableBalance

        return if (amount > BigDecimal(5)) {
            tuple(currency, BigDecimal(5))
        } else {
            availableBalance
        }
    }

    companion object {
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

        private fun updateMarketsWithBestPath(markets: Array<TranIntentMarket>, marketIdx: Int, bestPath: Array<TranIntentMarket>): Array<TranIntentMarket> {
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

        private fun isAdjustmentTrade(trade: BareTrade): Boolean {
            return trade.price.compareTo(BigDecimal.ONE) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0
                || trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ONE) == 0
                || trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0
        }

        private fun adjustFromAmount(amount: Amount): BareTrade {
            return BareTrade(amount, BigDecimal.ONE, BigDecimal.ZERO)
        }

        private fun adjustTargetAmount(amount: Amount, orderType: OrderType): BareTrade {
            return if (orderType == OrderType.Buy) {
                BareTrade(amount, BigDecimal.ZERO, BigDecimal.ONE)
            } else {
                BareTrade(amount, BigDecimal.ZERO, BigDecimal.ZERO)
            }
        }

        private fun sellBaseAmountAdj(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
            return if (price.compareTo(BigDecimal.ZERO) == 0 && feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
                quoteAmount
            } else {
                sellBaseAmount(quoteAmount, price, feeMultiplier)
            }
        }

        private fun splitTradeHandleAdjustment(trade: BareTrade, fromOrToAmount: Amount): Tuple2<BareTrade, List<BareTrade>> {
            val q0 = fromOrToAmount
            val q1 = trade.quoteAmount - q0

            if (q1 < BigDecimal.ZERO) throw RuntimeException("Adjustment trade $trade is not supported")

            val commitTrade = BareTrade(q0, trade.price, trade.feeMultiplier)
            val updateTrade = BareTrade(q1, trade.price, trade.feeMultiplier)

            return tuple(commitTrade, list(updateTrade))
        }

        private fun splitTradeForwardBuy(trade: BareTrade, fromAmount: Amount): Tuple2<BareTrade, List<BareTrade>> {
            if (isAdjustmentTrade(trade)) return splitTradeHandleAdjustment(trade, fromAmount)

            val tradeWithdraw = buyBaseAmount(trade.quoteAmount, trade.price)
            val tradeDeposit = buyQuoteAmount(trade.quoteAmount, trade.feeMultiplier)

            val commitQuoteAmount = fromAmount.divide(trade.price, 8, RoundingMode.DOWN)
            val updateQuoteAmount = (tradeWithdraw - fromAmount).divide(trade.price, 8, RoundingMode.DOWN)

            val withdrawDelta = tradeWithdraw - buyBaseAmount(commitQuoteAmount, trade.price) - buyBaseAmount(updateQuoteAmount, trade.price)
            val depositDelta = tradeDeposit - buyQuoteAmount(commitQuoteAmount, trade.feeMultiplier) - buyQuoteAmount(updateQuoteAmount, trade.feeMultiplier)

            val updateTrade = BareTrade(updateQuoteAmount, trade.price, trade.feeMultiplier)
            val commitTrade = BareTrade(commitQuoteAmount, trade.price, trade.feeMultiplier)

            val commitTradeWithdrawAdj = if (withdrawDelta.compareTo(BigDecimal.ZERO) != 0) adjustFromAmount(withdrawDelta) else null
            val commitTradeDepositAdj = if (depositDelta.compareTo(BigDecimal.ZERO) != 0) adjustTargetAmount(depositDelta, OrderType.Buy) else null

            val commitTrades = listOfNotNull(commitTrade, commitTradeWithdrawAdj, commitTradeDepositAdj).toVavrList()

            return tuple(updateTrade, commitTrades)
        }

        private fun splitTradeForwardSell(trade: BareTrade, fromAmount: Amount): Tuple2<BareTrade, List<BareTrade>> {
            if (isAdjustmentTrade(trade)) return splitTradeHandleAdjustment(trade, fromAmount)

            val tradeWithdraw = sellQuoteAmount(trade.quoteAmount)
            val tradeDeposit = sellBaseAmount(trade.quoteAmount, trade.price, trade.feeMultiplier)

            val commitQuoteAmount = fromAmount
            val updateQuoteAmount = tradeWithdraw - fromAmount

            val depositDelta = run {
                val a = sellBaseAmount(commitQuoteAmount, trade.price, trade.feeMultiplier)
                val b = sellBaseAmount(updateQuoteAmount, trade.price, trade.feeMultiplier)
                tradeDeposit - a - b
            }

            val updateTrade = BareTrade(updateQuoteAmount, trade.price, trade.feeMultiplier)
            val commitTrade = BareTrade(commitQuoteAmount, trade.price, trade.feeMultiplier)

            val commitTradeDepositAdj = if (depositDelta.compareTo(BigDecimal.ZERO) != 0) adjustTargetAmount(depositDelta, OrderType.Sell) else null

            val commitTrades = listOfNotNull(commitTrade, commitTradeDepositAdj).toVavrList()

            return tuple(updateTrade, commitTrades)
        }

        private fun splitTradeForward(trade: BareTrade, orderType: OrderType, fromAmount: Amount): Tuple2<BareTrade, List<BareTrade>> {
            return if (orderType == OrderType.Buy) {
                splitTradeForwardBuy(trade, fromAmount)
            } else {
                splitTradeForwardSell(trade, fromAmount)
            }
        }

        private fun splitTradeBackwardBuy(trade: BareTrade, targetAmount: Amount): Tuple2<BareTrade, List<BareTrade>> {
            if (isAdjustmentTrade(trade)) return splitTradeHandleAdjustment(trade, targetAmount)

            val tradeWithdraw = buyBaseAmount(trade.quoteAmount, trade.price)
            val tradeDeposit = buyQuoteAmount(trade.quoteAmount, trade.feeMultiplier)

            val commitQuoteAmount = targetAmount.divide(trade.feeMultiplier, 8, RoundingMode.DOWN)
            val updateQuoteAmount = (tradeDeposit - targetAmount).divide(trade.feeMultiplier, 8, RoundingMode.DOWN)

            val withdrawDelta = tradeWithdraw - buyBaseAmount(commitQuoteAmount, trade.price) - buyBaseAmount(updateQuoteAmount, trade.price)
            val depositDelta = tradeDeposit - buyQuoteAmount(commitQuoteAmount, trade.feeMultiplier) - buyQuoteAmount(updateQuoteAmount, trade.feeMultiplier)

            val updateTrade = BareTrade(updateQuoteAmount, trade.price, trade.feeMultiplier)
            val commitTrade = BareTrade(commitQuoteAmount, trade.price, trade.feeMultiplier)

            val commitTradeWithdrawAdj = if (withdrawDelta.compareTo(BigDecimal.ZERO) != 0) adjustFromAmount(withdrawDelta) else null
            val commitTradeDepositAdj = if (depositDelta.compareTo(BigDecimal.ZERO) != 0) adjustTargetAmount(depositDelta, OrderType.Buy) else null

            val commitTrades = listOfNotNull(commitTrade, commitTradeWithdrawAdj, commitTradeDepositAdj).toVavrList()

            return tuple(updateTrade, commitTrades)
        }

        private fun splitTradeBackwardSell(trade: BareTrade, targetAmount: Amount): Tuple2<BareTrade, List<BareTrade>> {
            if (isAdjustmentTrade(trade)) return splitTradeHandleAdjustment(trade, targetAmount)

            val tradeWithdraw = sellQuoteAmount(trade.quoteAmount)
            val tradeDeposit = sellBaseAmount(trade.quoteAmount, trade.price, trade.feeMultiplier)

            val (commitQuoteAmount, updateQuoteAmount) = run {
                val pf = (trade.price * trade.feeMultiplier).setScale(8, RoundingMode.DOWN)
                val cQ = targetAmount.divide(pf, 8, RoundingMode.DOWN)
                val uQ = (tradeDeposit - targetAmount).divide(pf, 8, RoundingMode.DOWN)
                tuple(cQ, uQ)
            }

            val withdrawDelta = tradeWithdraw - sellQuoteAmount(commitQuoteAmount) - sellQuoteAmount(updateQuoteAmount)
            val depositDelta = run {
                val a = sellBaseAmount(commitQuoteAmount, trade.price, trade.feeMultiplier)
                val b = sellBaseAmount(updateQuoteAmount, trade.price, trade.feeMultiplier)
                tradeDeposit - a - b
            }

            val updateTrade = BareTrade(updateQuoteAmount, trade.price, trade.feeMultiplier)
            val commitTrade = BareTrade(commitQuoteAmount, trade.price, trade.feeMultiplier)

            val commitTradeWithdrawAdj = if (withdrawDelta.compareTo(BigDecimal.ZERO) != 0) adjustFromAmount(withdrawDelta) else null
            val commitTradeDepositAdj = if (depositDelta.compareTo(BigDecimal.ZERO) != 0) adjustTargetAmount(depositDelta, OrderType.Sell) else null

            val commitTrades = listOfNotNull(commitTrade, commitTradeWithdrawAdj, commitTradeDepositAdj).toVavrList()

            return tuple(updateTrade, commitTrades)
        }

        private fun splitTradeBackward(trade: BareTrade, orderType: OrderType, targetAmount: Amount): Tuple2<BareTrade, List<BareTrade>> {
            return if (orderType == OrderType.Buy) {
                splitTradeBackwardBuy(trade, targetAmount)
            } else {
                splitTradeBackwardSell(trade, targetAmount)
            }
        }

        private fun splitMarkets(markets: Array<TranIntentMarket>, currentMarketIdx: Int, trades: Array<BareTrade>): Tuple2<Array<TranIntentMarket>, Array<TranIntentMarket>> {
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
                    val amount = if (m.orderType == OrderType.Buy) {
                        buyQuoteAmount(trade.quoteAmount, trade.feeMultiplier)
                    } else {
                        sellBaseAmountAdj(trade.quoteAmount, trade.price, trade.feeMultiplier)
                    }

                    if (amount <= targetAmount) {
                        committedTrades.add(trade)
                        targetAmount -= amount
                    } else {
                        if (targetAmount.compareTo(BigDecimal.ZERO) == 0) {
                            updatedTrades.add(trade)
                        } else {
                            val (l, r) = splitTradeBackward(trade, m.orderType, targetAmount)
                            updatedTrades.add(l)
                            committedTrades.addAll(r)
                            targetAmount = BigDecimal.ZERO
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

        private fun mergeMarkets(currentMarkets: Array<TranIntentMarket>, unfilledMarkets: List<Tuple2<Amount, Amount>>?): Array<TranIntentMarket> {
            if (unfilledMarkets == null || unfilledMarkets.length() == 0) return currentMarkets

            var newMarkets = currentMarkets

            for (amounts in unfilledMarkets) {
                newMarkets = mergeMarkets(newMarkets, amounts._1, amounts._2)
            }

            return newMarkets
        }

        private fun mergeMarkets(currentMarkets: Array<TranIntentMarket>, initCurrencyAmount: Amount, currentCurrencyAmount: Amount): Array<TranIntentMarket> {
            var updatedMarkets = currentMarkets
            val currMarketIdx = partiallyCompletedMarketIndex(currentMarkets)!!
            val prevMarketIdx = currMarketIdx - 1

            val oldCurrentMarket = updatedMarkets[currMarketIdx] as TranIntentMarketPartiallyCompleted
            var targetAmount = if (prevMarketIdx >= 0) {
                (updatedMarkets[prevMarketIdx] as TranIntentMarketCompleted).targetAmount + currentCurrencyAmount
            } else {
                oldCurrentMarket.fromAmount + initCurrencyAmount + currentCurrencyAmount
            }

            // 1. Update current market.
            val newCurrentMarket = TranIntentMarketPartiallyCompleted(
                oldCurrentMarket.market,
                oldCurrentMarket.orderSpeed,
                oldCurrentMarket.fromCurrencyType,
                targetAmount
            )
            updatedMarkets = updatedMarkets.update(currMarketIdx, newCurrentMarket)

            // 2. Update another markets.

            for (i in prevMarketIdx downTo 0) {
                val market = updatedMarkets[i] as TranIntentMarketCompleted
                val targetAmountDelta = targetAmount - market.targetAmount

                if (targetAmountDelta.compareTo(BigDecimal.ZERO) == 0) break

                if (market.orderType == OrderType.Buy) {
                    val price = market.trades.asSequence().filter { !isAdjustmentTrade(it) }.map { it.price }.max() ?: BigDecimal.ONE
                    val fee = market.trades.asSequence().filter { !isAdjustmentTrade(it) }.map { it.feeMultiplier }.max() ?: BigDecimal.ONE
                    val quoteAmount = targetAmountDelta.divide(fee, 8, RoundingMode.DOWN)
                    val trade = BareTrade(quoteAmount, price, fee)
                    var newTrades = market.trades.append(trade)
                    val targetAmountNew = newTrades.asSequence()
                        .map { buyQuoteAmount(it.quoteAmount, it.feeMultiplier) }
                        .fold(BigDecimal.ZERO, { a, b -> a + b })
                    val targetAmountNewDelta = targetAmount - targetAmountNew
                    if (targetAmountNewDelta.compareTo(BigDecimal.ZERO) != 0) {
                        val adjTrade = adjustTargetAmount(targetAmountNewDelta, OrderType.Buy)
                        newTrades = newTrades.append(adjTrade)
                    }
                    val newMarket = TranIntentMarketCompleted(market.market, market.orderSpeed, market.fromCurrencyType, newTrades)
                    updatedMarkets = updatedMarkets.update(i, newMarket)
                    targetAmount = newMarket.fromAmount
                } else {
                    val price = market.trades.asSequence().filter { !isAdjustmentTrade(it) }.map { it.price }.min() ?: BigDecimal.ONE
                    val fee = market.trades.asSequence().filter { !isAdjustmentTrade(it) }.map { it.feeMultiplier }.min() ?: BigDecimal.ONE
                    val quoteAmount = targetAmountDelta.divide(price, 8, RoundingMode.UP).divide(fee, 8, RoundingMode.UP)
                    val trade = BareTrade(quoteAmount, price, fee)
                    var newTrades = market.trades.append(trade)
                    val targetAmountNew = newTrades.asSequence()
                        .map {
                            if (it.price.compareTo(BigDecimal.ZERO) == 0 && it.quoteAmount.compareTo(BigDecimal.ZERO) == 0)
                                it.quoteAmount
                            else
                                sellBaseAmount(it.quoteAmount, it.price, it.feeMultiplier)
                        }
                        .fold(BigDecimal.ZERO, { a, b -> a + b })
                    val targetAmountNewDelta = targetAmount - targetAmountNew
                    if (targetAmountNewDelta.compareTo(BigDecimal.ZERO) != 0) {
                        val adjTrade = adjustTargetAmount(targetAmountNewDelta, OrderType.Sell)
                        newTrades = newTrades.append(adjTrade)
                    }
                    val newMarket = TranIntentMarketCompleted(market.market, market.orderSpeed, market.fromCurrencyType, newTrades)
                    updatedMarkets = updatedMarkets.update(i, newMarket)
                    targetAmount = newMarket.fromAmount
                }
            }

            // 3. Add an amount to trades of init market.

            if (prevMarketIdx >= 0 && initCurrencyAmount.compareTo(BigDecimal.ZERO) != 0) {
                val initMarket = updatedMarkets[0] as TranIntentMarketCompleted

                val fromAmountAllInitial = (currentMarkets[0] as TranIntentMarketCompleted).fromAmount
                val fromAmountCalculated = initMarket.fromAmount
                val deltaAmount = fromAmountAllInitial + initCurrencyAmount - fromAmountCalculated

                val firstTradeIdx = initMarket.trades.asSequence()
                    .mapIndexed { i, trade -> tuple(i, trade) }
                    .filter { !isAdjustmentTrade(it._2) }
                    .map { (i, trade) ->
                        val fromAmount = if (initMarket.orderType == OrderType.Buy) {
                            buyBaseAmount(trade.quoteAmount, trade.price)
                        } else {
                            sellQuoteAmount(trade.quoteAmount)
                        }
                        tuple(i, trade, fromAmount + deltaAmount)
                    }
                    .filter { it._3 > BigDecimal.ZERO }
                    .sortedByDescending { it._3 }
                    .map { it._1 }
                    .firstOrNull()

                if (deltaAmount.compareTo(BigDecimal.ZERO) != 0 && firstTradeIdx != null) {
                    val trade = initMarket.trades[firstTradeIdx]
                    val newTradesList = if (initMarket.orderType == OrderType.Buy) {
                        val fromAmount = buyBaseAmount(trade.quoteAmount, trade.price)
                        val newFromAmount = fromAmount + deltaAmount
                        val newPrice = newFromAmount.divide(trade.quoteAmount, 8, RoundingMode.DOWN)
                        val newTrade = BareTrade(trade.quoteAmount, newPrice, trade.feeMultiplier)
                        val calcNewFromAmount = buyBaseAmount(newTrade.quoteAmount, newTrade.price)
                        val newFromAmountDelta = newFromAmount - calcNewFromAmount
                        if (newFromAmountDelta.compareTo(BigDecimal.ZERO) != 0) {
                            val adjTrade = adjustFromAmount(newFromAmountDelta)
                            listOf(newTrade, adjTrade)
                        } else {
                            listOf(newTrade)
                        }
                    } else {
                        val tradeFromAmount = sellQuoteAmount(trade.quoteAmount)
                        val tradeTargetAmount = if (trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.quoteAmount.compareTo(BigDecimal.ZERO) == 0) {
                            trade.quoteAmount
                        } else {
                            sellBaseAmount(trade.quoteAmount, trade.price, trade.feeMultiplier)
                        }
                        val newFromAmount = tradeFromAmount + deltaAmount
                        val newPrice = tradeTargetAmount.divide(newFromAmount, 8, RoundingMode.DOWN).divide(trade.feeMultiplier, 8, RoundingMode.DOWN)
                        val newTrade = BareTrade(newFromAmount, newPrice, trade.feeMultiplier)
                        val calcNewTargetAmount = sellBaseAmount(newTrade.quoteAmount, newTrade.price, newTrade.feeMultiplier)
                        val newTargetAmountDelta = tradeTargetAmount - calcNewTargetAmount
                        if (newTargetAmountDelta.compareTo(BigDecimal.ZERO) != 0) {
                            val adjTrade = adjustTargetAmount(newTargetAmountDelta, OrderType.Sell)
                            listOf(newTrade, adjTrade)
                        } else {
                            listOf(newTrade)
                        }
                    }
                    val newTrades = initMarket.trades.removeAt(firstTradeIdx).appendAll(newTradesList)
                    val newMarket = TranIntentMarketCompleted(initMarket.market, initMarket.orderSpeed, initMarket.fromCurrencyType, newTrades)
                    updatedMarkets = updatedMarkets.update(0, newMarket)
                } else {
                    val newTrade = adjustFromAmount(currentCurrencyAmount)
                    val newTrades = initMarket.trades.append(newTrade)
                    val newMarket = TranIntentMarketCompleted(initMarket.market, initMarket.orderSpeed, initMarket.fromCurrencyType, newTrades)
                    updatedMarkets = updatedMarkets.update(0, newMarket)
                }
            }

            return updatedMarkets
        }

        private suspend fun simulateInstantTrades(
            fromAmount: Amount,
            orderType: OrderType,
            orderBookFlow: Flow<OrderBookAbstract>,
            feeFlow: Flow<FeeMultiplier>
        ): Flow<Tuple2<Amount, BareTrade>> = flow {
            val orderBook = orderBookFlow.first()
            val fee = feeFlow.first()

            var unusedFromAmount = fromAmount

            if (orderType == OrderType.Buy) {
                if (orderBook.asks.length() == 0) throw OrderBookEmptyException(SubBookType.Sell)

                for ((basePrice, quoteAmount) in orderBook.asks) {
                    val availableFromAmount = buyBaseAmount(quoteAmount, basePrice)

                    if (unusedFromAmount <= availableFromAmount) {
                        val tradeQuoteAmount = calcQuoteAmount(unusedFromAmount, basePrice)
                        val trade = BareTrade(tradeQuoteAmount, basePrice, fee.taker)
                        unusedFromAmount = BigDecimal.ZERO
                        emit(tuple(unusedFromAmount, trade))
                        break
                    } else {
                        unusedFromAmount -= availableFromAmount
                        val trade = BareTrade(quoteAmount, basePrice, fee.taker)
                        emit(tuple(unusedFromAmount, trade))
                    }
                }
            } else {
                if (orderBook.bids.length() == 0) throw OrderBookEmptyException(SubBookType.Buy)

                for ((basePrice, quoteAmount) in orderBook.bids) {
                    if (unusedFromAmount <= quoteAmount) {
                        val trade = BareTrade(unusedFromAmount, basePrice, fee.taker)
                        unusedFromAmount = BigDecimal.ZERO
                        emit(tuple(unusedFromAmount, trade))
                        break
                    } else {
                        unusedFromAmount -= sellQuoteAmount(quoteAmount)
                        val trade = BareTrade(quoteAmount, basePrice, fee.taker)
                        emit(tuple(unusedFromAmount, trade))
                    }
                }
            }
        }
    }

    inner class TransactionIntent(
        val id: PathId,
        val markets: Array<TranIntentMarket>,
        val marketIdx: Int,
        private val TranIntentScope: CoroutineScope
    ) {
        private val fromAmountInputChannel = Channel<Tuple3<Amount, Amount, CompletableDeferred<Boolean>>>()
        private val generalMutex = Mutex()

        fun start(): Job = TranIntentScope.launch(CoroutineName("INTENT $id")) {
            logger.debug { "Start intent ($marketIdx) ${markets.pathString()}" }

            val merged = withContext(NonCancellable) {
                val existingIntent = intentManager.get(markets, marketIdx)
                if (existingIntent != null) {
                    val initFromAmount = markets[0].fromAmount(markets, 0)
                    val currentFromAmount = markets[marketIdx].fromAmount(markets, marketIdx)

                    logger.debug { "Existing intent found. Trying to merge current intent with an amount ($initFromAmount, $currentFromAmount) into ${existingIntent.id}..." }

                    val merged = existingIntent.merge(initFromAmount, currentFromAmount)

                    if (merged) {
                        transactionsDao.deleteActive(id)
                        logger.debug { "Current intent has been merged into ${existingIntent.id}" }
                    } else {
                        logger.debug { "Current intent has not been merged into ${existingIntent.id}" }
                    }

                    merged
                } else {
                    logger.debug("Merge intent has not been found for current intent")

                    false
                }
            }

            if (merged) return@launch

            intentManager.add(this@TransactionIntent)

            logger.debug { "Starting path traversal" }

            val currentMarket = markets[marketIdx] as TranIntentMarketPartiallyCompleted
            val orderBookFlow = data.getOrderBookFlowBy(currentMarket.market)
            val feeFlow = data.fee
            val newMarketIdx = marketIdx + 1
            var modifiedMarkets = withContext(NonCancellable) {
                TransactionalOperator.create(tranManager, object : TransactionDefinition {
                    override fun getIsolationLevel() = TransactionDefinition.ISOLATION_REPEATABLE_READ
                }).transactional(mono(Dispatchers.Unconfined) {
                    val unfilledMarkets = unfilledMarketsDao.get(primaryCurrencies, currentMarket.fromCurrency)

                    val modifiedMarkets = mergeMarkets(markets, unfilledMarkets)

                    if (unfilledMarkets.length() != 0) {
                        transactionsDao.updateActive(id, modifiedMarkets, marketIdx)
                        unfilledMarketsDao.remove(primaryCurrencies, currentMarket.fromCurrency)

                        logger.debug { "Merged unfilled amounts $unfilledMarkets into current intent. Before: $markets ; After: $modifiedMarkets" }
                    }

                    modifiedMarkets
                }).retry().awaitFirst()
            }

            if (currentMarket.orderSpeed == OrderSpeed.Instant) {
                withContext(NonCancellable) {
                    generalMutex.withLock {
                        while (!fromAmountInputChannel.isEmpty) {
                            val (initFromAmount, currFromAmount, approve) = fromAmountInputChannel.receive()
                            val prevMarkets = modifiedMarkets
                            modifiedMarkets = mergeMarkets(prevMarkets, initFromAmount, currFromAmount)
                            transactionsDao.updateActive(id, modifiedMarkets, marketIdx)
                            approve.complete(true)
                            logger.debug { "Merged amount (init = $initFromAmount, current = $currFromAmount) into $prevMarkets => $modifiedMarkets" }
                        }
                        fromAmountInputChannel.close()
                    }
                }

                val fromAmount = (modifiedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount
                val forceCancel = AtomicBoolean(false)
                val forceCancelJob = launch(start = CoroutineStart.UNDISPATCHED) {
                    try {
                        delay(Long.MAX_VALUE)
                    } finally {
                        forceCancel.set(true)
                    }
                }

                withContext(NonCancellable) {
                    val trades = try {
                        val delayedProcessor = delayedTradeManager.get(currentMarket.market, !currentMarket.orderType)

                        try {
                            delayedProcessor.pause()
                            tradeInstantly(currentMarket.market, currentMarket.orderType, fromAmount, orderBookFlow, feeFlow, forceCancel)
                        } finally {
                            delayedProcessor.resume()
                            forceCancelJob.cancel()
                        }
                    } catch (_: CancellationException) {
                        if (marketIdx == 0) {
                            transactionsDao.deleteActive(id)
                            intentManager.remove(id)

                            logger.debug { "Removing current instant intent from all places" }
                        }

                        return@withContext
                    } catch (e: Throwable) {
                        logger.debug { "Instant ${currentMarket.orderType} has been completed with error: ${e.message}." }

                        val fromCurrencyInit = modifiedMarkets[0].fromCurrency
                        val fromCurrencyInitAmount = modifiedMarkets[0].fromAmount(modifiedMarkets, 0)
                        val fromCurrencyCurrent = modifiedMarkets[marketIdx].fromCurrency
                        val fromCurrencyCurrentAmount = modifiedMarkets[marketIdx].fromAmount(modifiedMarkets, marketIdx)

                        val primaryCurrencyUnfilled = primaryCurrencies.contains(fromCurrencyCurrent)
                            && primaryCurrencies.contains(fromCurrencyInit)
                            && fromCurrencyInitAmount <= fromCurrencyCurrentAmount

                        TransactionalOperator.create(tranManager).transactional(mono(Dispatchers.Unconfined) {
                            transactionsDao.deleteActive(id)

                            if (!primaryCurrencyUnfilled) {
                                unfilledMarketsDao.add(fromCurrencyInit, fromCurrencyInitAmount, fromCurrencyCurrent, fromCurrencyCurrentAmount)

                                logger.debug {
                                    "Added ($marketIdx) ${modifiedMarkets.pathString()} " +
                                        "init=($fromCurrencyInit, $fromCurrencyInitAmount), " +
                                        "current=($fromCurrencyCurrent, $fromCurrencyCurrentAmount) to unfilled markets "
                                }
                            }
                        }).retry().awaitFirstOrNull()

                        intentManager.remove(id)

                        return@withContext
                    }

                    logger.debug { "Instant ${currentMarket.orderType} has been completed. Trades: $trades}" }

                    if (logger.isDebugEnabled && soundSignalEnabled) SoundUtil.beep()

                    val (unfilledTradeMarkets, committedMarkets) = splitMarkets(modifiedMarkets, marketIdx, trades)

                    logger.debug {
                        "Splitting markets (markets = $modifiedMarkets, idx = $marketIdx, trades = $trades) => " +
                            "[updatedMarkets = $unfilledTradeMarkets], [committedMarkets = $committedMarkets]"
                    }

                    val unfilledFromAmount = unfilledTradeMarkets[marketIdx].fromAmount(unfilledTradeMarkets, marketIdx)

                    if (unfilledFromAmount.compareTo(BigDecimal.ZERO) != 0 && marketIdx != 0) {
                        val fromCurrencyInit = unfilledTradeMarkets[0].fromCurrency
                        val fromCurrencyInitAmount = unfilledTradeMarkets[0].fromAmount(unfilledTradeMarkets, 0)
                        val fromCurrencyCurrent = unfilledTradeMarkets[marketIdx].fromCurrency

                        val primaryCurrencyUnfilled = primaryCurrencies.contains(fromCurrencyCurrent)
                            && primaryCurrencies.contains(fromCurrencyInit)
                            && fromCurrencyInitAmount <= unfilledFromAmount

                        if (!primaryCurrencyUnfilled) {
                            unfilledMarketsDao.add(fromCurrencyInit, fromCurrencyInitAmount, fromCurrencyCurrent, unfilledFromAmount)

                            logger.debug { "Added ($marketIdx) ${modifiedMarkets.pathString()} [init = ($fromCurrencyInit, $fromCurrencyInitAmount)], [current = ($fromCurrencyCurrent, $unfilledFromAmount)] to unfilled markets " }
                        }
                    }

                    if (newMarketIdx != modifiedMarkets.length()) {
                        transactionsDao.updateActive(id, committedMarkets, newMarketIdx)
                        TransactionIntent(id, committedMarkets, newMarketIdx, TranIntentScope).start()
                    } else {
                        TransactionalOperator.create(tranManager).transactional(mono(Dispatchers.Unconfined) {
                            transactionsDao.addCompleted(id, committedMarkets)
                            transactionsDao.deleteActive(id)
                            intentManager.remove(id)
                        }).retry().awaitFirstOrNull()
                    }
                }
            } else {
                var updatedMarkets = modifiedMarkets
                val updatedMarketsRef = AtomicReference(updatedMarkets)
                val cancelByProfitMonitoringJob = AtomicBoolean(false)
                val profitMonitoringJob = startProfitMonitoring(updatedMarketsRef, cancelByProfitMonitoringJob)
                var finishedWithError = false

                val delayedProcessor = delayedTradeManager.get(currentMarket.market, currentMarket.orderType)

                val cancellationMonitorJob = launch {
                    try {
                        delay(Long.MAX_VALUE)
                    } finally {
                        withContext(NonCancellable) {
                            logger.debug { "Cancel event received for delayed trade. Trying to unregister intent..." }
                            delayedProcessor.unregister(id)
                        }
                    }
                }

                try {
                    val tradesChannel = Channel<List<BareTrade>>(Channel.UNLIMITED)
                    delayedProcessor.register(id, tradesChannel)

                    launch {
                        withContext(NonCancellable) {
                            run {
                                val fromAmount = (updatedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount
                                logger.debug { "Trying to add first amount $fromAmount of current intent to trade processor..." }

                                val approved = delayedProcessor.addAmount(id, fromAmount)

                                if (approved) {
                                    logger.debug { "First amount has been added to trade processor." }
                                } else {
                                    logger.debug { "First amount hasn't been added to trade processor." }
                                    return@withContext
                                }
                            }
                            for ((initFromAmount, currFromAmount, approve) in fromAmountInputChannel) {
                                generalMutex.withLock {
                                    logger.debug { "Intent has received some amount ($initFromAmount, $currFromAmount). Trying to add it to trade processor..." }

                                    val approved = delayedProcessor.addAmount(id, currFromAmount)

                                    if (approved) {
                                        updatedMarkets = mergeMarkets(updatedMarkets, initFromAmount, currFromAmount)
                                        updatedMarketsRef.set(updatedMarkets)
                                        transactionsDao.updateActive(id, updatedMarkets, marketIdx)
                                        logger.debug { "Amount ($initFromAmount, $currFromAmount) has been added to trade processor." }
                                    } else {
                                        logger.debug { "Amount ($initFromAmount, $currFromAmount) hasn't been added to trade processor." }
                                    }

                                    approve.complete(approved)
                                }
                            }
                        }
                    }

                    withContext(NonCancellable) {
                        tradesChannel
                            .asFlow()
                            .collect { receivedTrades ->
                                val trades = receivedTrades.toArray()

                                if (logger.isDebugEnabled) {
                                    logger.debug("Received delayed trades: $trades")
                                    if (soundSignalEnabled) SoundUtil.beep()
                                }

                                generalMutex.withLock {
                                    val oldMarkets = updatedMarkets
                                    val marketSplit = splitMarkets(oldMarkets, marketIdx, trades)

                                    updatedMarkets = marketSplit._1
                                    val committedMarkets = marketSplit._2

                                    logger.debug {
                                        "Splitting markets (markets = $oldMarkets, idx = $marketIdx, trades = $trades) => " +
                                            "[updatedMarkets = $updatedMarkets], [committedMarkets = $committedMarkets]"
                                    }

                                    val fromAmount = (updatedMarkets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount

                                    if (fromAmount.compareTo(BigDecimal.ZERO) == 0) {
                                        logger.debug { "fromAmount is zero. Cancelling profit monitoring job..." }

                                        profitMonitoringJob.cancelAndJoin()
                                        cancelByProfitMonitoringJob.set(false)

                                        logger.debug { "Profit monitoring job has been cancelled." }
                                    } else {
                                        logger.debug { "fromAmount isn't zero $fromAmount. Continue..." }

                                        updatedMarketsRef.set(updatedMarkets)
                                    }

                                    if (newMarketIdx != updatedMarkets.length()) {
                                        val newId = UUID.randomUUID()

                                        TransactionalOperator.create(tranManager).transactional(mono(Dispatchers.Unconfined) {
                                            if (fromAmount.compareTo(BigDecimal.ZERO) != 0) {
                                                transactionsDao.updateActive(id, updatedMarkets, marketIdx)
                                            }

                                            transactionsDao.addActive(newId, committedMarkets, newMarketIdx)
                                        }).retry().awaitFirstOrNull()

                                        TransactionIntent(newId, committedMarkets, newMarketIdx, TranIntentScope).start()
                                    } else {
                                        transactionsDao.addCompleted(id, committedMarkets)
                                    }
                                }
                            }

                        logger.debug { "Trades channel has been closed successfully" }
                    }
                } catch (e: CancellationException) {
                } catch (e: Throwable) {
                    finishedWithError = true
                    logger.debug { "Path has finished with exception: ${e.message}" }
                } finally {
                    val isCancelled = !isActive

                    withContext(NonCancellable) {
                        cancellationMonitorJob.cancelAndJoin()

                        generalMutex.withLock {
                            fromAmountInputChannel.close()
                        }

                        delayedProcessor.unregister(id)

                        if (cancelByProfitMonitoringJob.get()) {
                            logger.debug { "Cancelled by profit monitoring job." }
                            return@withContext
                        }

                        profitMonitoringJob.cancel()

                        // Handle an unfilled amount
                        val initMarket = updatedMarkets[0]
                        val currMarket = updatedMarkets[marketIdx]
                        val fromAmount = currMarket.fromAmount(updatedMarkets, marketIdx)

                        if (marketIdx != 0 && fromAmount.compareTo(BigDecimal.ZERO) != 0) {
                            if (!isCancelled || finishedWithError) {
                                val fromCurrencyInit = initMarket.fromCurrency
                                val fromCurrencyInitAmount = initMarket.fromAmount(updatedMarkets, 0)
                                val fromCurrencyCurrent = currMarket.fromCurrency
                                val fromCurrencyCurrentAmount = currMarket.fromAmount(updatedMarkets, marketIdx)

                                val primaryCurrencyUnfilled = primaryCurrencies.contains(fromCurrencyCurrent)
                                    && primaryCurrencies.contains(fromCurrencyInit)
                                    && fromCurrencyInitAmount <= fromCurrencyCurrentAmount

                                intentManager.remove(id)

                                if (primaryCurrencyUnfilled) {
                                    transactionsDao.deleteActive(id)
                                } else {
                                    val existingIntentCandidate = intentManager.get(updatedMarkets, marketIdx)
                                    val mergedToSimilarIntent = existingIntentCandidate?.merge(fromCurrencyInitAmount, fromCurrencyCurrentAmount)

                                    if (mergedToSimilarIntent == true) {
                                        transactionsDao.deleteActive(id)
                                        logger.debug { "Current intent has been merged into ${existingIntentCandidate.id}" }
                                    } else {
                                        TransactionalOperator.create(tranManager).transactional(mono(Dispatchers.Unconfined) {
                                            transactionsDao.deleteActive(id)
                                            unfilledMarketsDao.add(fromCurrencyInit, fromCurrencyInitAmount, fromCurrencyCurrent, fromCurrencyCurrentAmount)
                                        }).retry().awaitFirstOrNull()

                                        logger.debug {
                                            "Added to unfilled markets " +
                                            "[init = ($fromCurrencyInit, $fromCurrencyInitAmount)], " +
                                            "[current = ($fromCurrencyCurrent, $fromCurrencyCurrentAmount)]"
                                        }
                                    }
                                }
                            } else {
                                logger.debug { "Current intent has not been added to unfilled markets because it is cancelled and finished without error " }
                            }
                        } else {
                            transactionsDao.deleteActive(id)
                            intentManager.remove(id)

                            logger.debug { "Removed intent from all places because marketIdx == 0 || fromAmount == 0" }
                        }
                    }
                }
            }
        }

        private suspend fun merge(initFromAmount: Amount, currentFromAmount: Amount): Boolean {
            return withContext(NonCancellable) {
                var merged: CompletableDeferred<Boolean>? = null
                generalMutex.withLock {
                    if (fromAmountInputChannel.isClosedForSend) return@withContext false
                    merged = CompletableDeferred()
                    fromAmountInputChannel.send(tuple(initFromAmount, currentFromAmount, merged!!))
                }
                merged!!.await()
            }
        }

        private suspend fun tradeInstantly(
            market: Market,
            orderType: OrderType,
            fromCurrencyAmount: Amount,
            orderBookFlow: Flow<OrderBookAbstract>,
            feeFlow: Flow<FeeMultiplier>,
            cancel: AtomicBoolean
        ): Array<BareTrade> {
            while (true) {
                if (cancel.get()) throw CancellationException()

                val feeMultiplier = feeFlow.first() // TODO: Remove when Poloniex will fix the bug with fee
                val simulatedInstantTrades = simulateInstantTrades(fromCurrencyAmount, orderType, orderBookFlow, feeFlow).toList(LinkedList())

                if (simulatedInstantTrades.isEmpty()) {
                    logger.warn("Can't do instant trade. Wait...")
                    delay(2000)
                    continue
                }

                logger.debug { "Simulated instant trades before execution: $simulatedInstantTrades" }

                val lastTradePrice = simulatedInstantTrades.last()._2.price

                val expectQuoteAmount = if (orderType == OrderType.Buy) {
                    calcQuoteAmount(fromCurrencyAmount, lastTradePrice)
                } else {
                    fromCurrencyAmount
                }

                if (expectQuoteAmount.compareTo(BigDecimal.ZERO) == 0) {
                    logger.debug("Quote amount for trade is equal to zero. From an amount: $fromCurrencyAmount")
                    throw AmountIsZeroException
                }

                val transaction = try {
                    logger.debug { "Trying to $orderType on market $market with price $lastTradePrice and amount $expectQuoteAmount" }
                    poloniexApi.placeLimitOrder(market, orderType, lastTradePrice, expectQuoteAmount, BuyOrderType.FillOrKill)
                } catch (e: UnableToFillOrderException) {
                    logger.debug(e.originalMsg)
                    delay(100)
                    continue
                } catch (e: TransactionFailedException) {
                    logger.debug(e.originalMsg)
                    delay(500)
                    continue
                } catch (e: NotEnoughCryptoException) {
                    logger.error(e.originalMsg)
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
                } catch (e: ConnectException) {
                    delay(2000)
                    continue
                } catch (e: SocketException) {
                    delay(2000)
                    continue
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    delay(2000)
                    if (logger.isDebugEnabled) logger.error(e.message)
                    continue
                }

                if (transaction.trades.size() == 0) {
                    logger.warn("Instant $orderType has successfully completed but trades list is empty. Self-trade occurred ?")
                    delay(1000)
                    continue
                }

                logger.debug { "Instant $orderType trades received: ${transaction.trades}" }

                return transaction.trades.toVavrStream().map { trade ->
                    val takerFee = if (transaction.feeMultiplier.compareTo(feeMultiplier.taker) != 0) {
                        logger.warn("Poloniex still has a bug with fees. Expected: ${feeMultiplier.taker}. Actual: ${transaction.feeMultiplier}")
                        feeMultiplier.taker
                    } else {
                        transaction.feeMultiplier
                    }

                    BareTrade(trade.amount, trade.price, takerFee)
                }.toArray()
            }
        }

        private fun CoroutineScope.startProfitMonitoring(
            updatedMarketsRef: AtomicReference<Array<TranIntentMarket>>,
            cancelByProfitMonitoringJob: AtomicBoolean
        ): Job {
            val parentJob = coroutineContext[Job]

            return launch(Job()) {
                val startTime = Instant.now()
                var counter = 0L

                while (isActive) {
                    delay(2000)
                    var updatedMarkets = updatedMarketsRef.get()

                    val initAmount = updatedMarkets.first().fromAmount(updatedMarkets, 0)
                    val targetAmount = updatedMarkets.last().targetAmount(updatedMarkets, updatedMarkets.length() - 1)

                    val profitable = initAmount < targetAmount
                    val timeout = Duration.between(startTime, Instant.now()).toMinutes() > 40

                    if (profitable && !timeout) {
                        if (counter++ % 15 == 0L) {
                            logger.debug { "Expected profit: +${targetAmount - initAmount}" }
                        }
                        continue
                    }

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
                        logger.debug { "Trying to find a new path..." }

                        bestPath = findNewPath(
                            initAmount,
                            fromCurrency,
                            fromCurrencyAmount,
                            primaryCurrencies,
                            updatedMarkets.length() - marketIdx
                        )

                        if (bestPath != null) {
                            logger.debug { "A new path found ${bestPath.pathString()}" }
                            break
                        } else {
                            logger.debug { "Path not found" }
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

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as TransactionIntent

            if (id != other.id) return false

            return true
        }

        override fun hashCode(): Int {
            return id.hashCode()
        }
    }

    private class TradeScheduler(private val orderType: OrderType) {
        private val ids = LinkedList<PathId>()
        private val idFromAmount = hashMapOf<PathId, Amount>()
        private val idOutput = hashMapOf<PathId, Channel<List<BareTrade>>>()
        private val idStatusNew = hashMapOf<PathId, Boolean>()
        private val mutex = Mutex()

        private val idFromAmountCommon = AtomicReference(BigDecimal.ZERO)

        val fromAmount: Amount
            get() {
                return idFromAmountCommon.get()
            }

        suspend fun register(id: PathId, outputTrades: Channel<List<BareTrade>>) {
            mutex.withLock {
                logger.debug { "Trying to register path in Trade Scheduler..." }

                ids.addLast(id)
                idStatusNew[id] = true
                idFromAmount[id] = BigDecimal.ZERO
                idOutput[id] = outputTrades

                logger.debug { "Path has been successfully registered in Trade Scheduler..." }
            }
        }

        suspend fun unregister(id: PathId) {
            mutex.withLock {
                logger.debug { "Trying to unregister path from Trade Scheduler..." }

                if (!pathExists(id)) {
                    logger.debug { "Path was already unregistered from Trade Scheduler earlier." }
                    return
                }

                ids.remove(id)
                idStatusNew.remove(id)
                idFromAmount.remove(id)
                idOutput.remove(id)?.close()

                recalculateCommonFromAmount()

                logger.debug { "Path has been successfully removed from Trade Scheduler" }
            }
        }

        suspend fun unregisterAll(error: Throwable? = null) {
            mutex.withLock {
                logger.debug { "Start unregistering all paths..." }
                for (id in ids.toVavrList()) {
                    if (idStatusNew[id] == true) continue

                    ids.remove(id)
                    idStatusNew.remove(id)
                    idFromAmount.remove(id)
                    idOutput.remove(id)?.close(error)

                    logger.debug { "Unregistered path $id" }
                }
                recalculateCommonFromAmount()
                logger.debug { "All paths have been unregistered" }
            }
        }

        fun pathExists(id: PathId): Boolean {
            return ids.contains(id)
        }

        suspend fun addAmount(id: PathId, fromAmount: Amount): Boolean {
            mutex.withLock {
                logger.debug { "Trying to add amount $fromAmount to trade scheduler..." }

                val added = run {
                    val output = idOutput[id]
                    val idAmount = idFromAmount[id]
                    if (output == null || idAmount == null || output.isClosedForSend) return@run false
                    idFromAmount[id] = idAmount + fromAmount
                    return@run true
                }

                if (added) {
                    idStatusNew[id] = false
                    recalculateCommonFromAmount()
                    logger.debug { "Amount $fromAmount has been added to trade scheduler" }
                } else {
                    logger.debug { "Amount $fromAmount has not been added to trade scheduler" }
                }

                return added
            }
        }

        suspend fun addTrades(tradeList: kotlin.collections.List<BareTrade>) {
            mutex.withLock {
                logger.debug { "Trying to add trades $tradeList to trade scheduler..." }

                val idTrade = mutableMapOf<PathId, MutableList<BareTrade>>()

                for (bareTrade in tradeList) {
                    var trade = bareTrade

                    var tradeFromAmount = if (orderType == OrderType.Buy) {
                        buyBaseAmount(trade.quoteAmount, trade.price)
                    } else {
                        sellQuoteAmount(trade.quoteAmount)
                    }

                    logger.debug { "Trying to find client for received trade $bareTrade..." }

                    for (id in ids) {
                        if (tradeFromAmount.compareTo(BigDecimal.ZERO) == 0) {
                            logger.debug { "tradeFromAmount is equal to zero. Exiting..." }
                            break
                        }

                        val idFromAmountValue = idFromAmount.getValue(id)

                        if (tradeFromAmount > idFromAmountValue) {
                            logger.debug { "Trying to split $id [tradeFromAmount ($tradeFromAmount) > idFromAmountValue ($idFromAmountValue)]" }

                            idFromAmount[id] = BigDecimal.ZERO
                            tradeFromAmount -= idFromAmountValue

                            val (lTrade, rTrades) = splitTradeForward(trade, orderType, idFromAmountValue)

                            logger.debug { "Split trade for $id: splitTrade(trade = $trade, orderType = $orderType, fromAmount = $idFromAmountValue) => (lTrade = $lTrade, rTrade = $rTrades)" }

                            trade = lTrade
                            idTrade.getOrPut(id, { mutableListOf() }).addAll(rTrades)
                        } else {
                            logger.debug { "Path $id matched received trade [tradeFromAmount ($tradeFromAmount) <= idFromAmountValue ($idFromAmountValue)]" }

                            val newIdFromAmount = idFromAmountValue - tradeFromAmount
                            idFromAmount[id] = newIdFromAmount
                            tradeFromAmount = BigDecimal.ZERO

                            idTrade.getOrPut(id, { mutableListOf() }).add(trade)
                        }
                    }

                    if (tradeFromAmount.compareTo(BigDecimal.ZERO) != 0) {
                        logger.error("No receiver found for received trade $trade.")
                    }
                }

                idTrade.forEach { (id, trades) ->
                    val output = idOutput.getValue(id)
                    val fromAmount = idFromAmount[id]!!

                    logger.debug { "Sending trade $trades to $id..." }
                    output.send(trades.toVavrList())
                    logger.debug { "Path $id has been processed the trade $trades" }

                    if (fromAmount.compareTo(BigDecimal.ZERO) == 0) {
                        logger.debug { "Path's amount $id is equal to zero. Closing output channel..." }
                        output.close()
                        logger.debug { "Output channel for $id has been closed" }
                    }
                }

                logger.debug { "All trades $tradeList have been processed by a trade scheduler." }

                recalculateCommonFromAmount()
            }
        }

        private fun recalculateCommonFromAmount() {
            val newFromAmount = idFromAmount.asSequence().map { it.value }.fold(BigDecimal.ZERO) { a, b -> a + b }
            idFromAmountCommon.set(newFromAmount)
        }

        companion object {
            private val logger = KotlinLogging.logger {}
        }
    }

    private inner class DelayedTradeProcessor(
        val market: Market,
        val orderType: OrderType,
        private val orderBook: Flow<OrderBookAbstract>,
        private val scope: CoroutineScope
    ) {
        private val scheduler = TradeScheduler(orderType)
        private var tradeWorkerJob: Job? = null
        private val mutex = Mutex()

        fun start(): Job = scope.launch(start = CoroutineStart.UNDISPATCHED) {
            try {
                delay(Long.MAX_VALUE)
            } finally {
                withContext(NonCancellable) {
                    mutex.withLock {
                        stopTradeWorker()
                        scheduler.unregisterAll()
                    }
                }
            }
        }

        suspend fun register(id: PathId, outputTrades: Channel<List<BareTrade>>) {
            mutex.withLock {
                scheduler.register(id, outputTrades)
            }
        }

        suspend fun unregister(id: PathId) {
            mutex.withLock {
                if (!scheduler.pathExists(id)) {
                    logger.debug { "Unregister is not required because path has already been removed" }
                    return
                }

                stopTradeWorker()
                scheduler.unregister(id)
                startTradeWorker()
            }
        }

        suspend fun addAmount(id: PathId, fromAmount: Amount): Boolean {
            return mutex.withLock {
                stopTradeWorker()
                val amountAdded = scheduler.addAmount(id, fromAmount)
                startTradeWorker()
                amountAdded
            }
        }

        suspend fun pause() {
            logger.debug("Trying to pause Delayed Trade Processor..")
            mutex.lock()
            stopTradeWorker()
        }

        fun resume() {
            startTradeWorker()
            mutex.unlock()
            logger.debug("Delayed Trade Processor has been successfully resumed")
        }

        private suspend fun stopTradeWorker() {
            logger.debug { "Trying to stop Delayed Trade Processor..." }
            tradeWorkerJob?.cancelAndJoin()
            tradeWorkerJob = null
            logger.debug { "Delayed Trade Processor has been stopped" }
        }

        private fun startTradeWorker() {
            if (tradeWorkerJob != null || scheduler.fromAmount.compareTo(BigDecimal.ZERO) == 0) return

            val workerJob = launchTradeWorker()
            tradeWorkerJob = workerJob
            scope.launch(Job(), CoroutineStart.UNDISPATCHED) {
                workerJob.join()
                logger.debug { "Delayed Trade Processor has been stopped" }

                mutex.withLock {
                    if (tradeWorkerJob == workerJob) tradeWorkerJob = null
                    startTradeWorker()
                }
            }
            logger.debug { "Delayed Trade Processor has been launched" }
        }

        private fun launchTradeWorker(): Job = scope.launch(Job()) {
            val thisJob = coroutineContext[Job]
            var prevOrderId: Long? = null
            var currOrderId: Long? = null
            var latestTradeId: Long = -1
            var prevPrice: Price? = null
            var prevQuoteAmount: Amount? = null
            val orderCreateConfirmed = AtomicReference<CompletableDeferred<Unit>?>(null)
            val orderCancelConfirmed = AtomicReference<CompletableDeferred<OrderUpdateType>?>(null)
            val commonStateMutex = Mutex()

            // TODO: Rethink recover mechanism
            // recoverAfterPowerOff()

            val tradeMonitoringJob = launch(Job(), CoroutineStart.UNDISPATCHED) {
                poloniexApi.accountNotificationStream.collect { notifications ->
                    var tradeList: LinkedList<BareTrade>? = null
                    var orderCreateReceived: LimitOrderCreated? = null
                    var orderCancelReceived: OrderUpdate? = null

                    withContext(NonCancellable) {
                        commonStateMutex.withLock {
                            for (notification in notifications) {
                                when (notification) {
                                    is TradeNotification -> run {
                                        if (currOrderId != null && notification.orderId == currOrderId || prevOrderId != null && notification.orderId == prevOrderId) {
                                            if (notification.tradeId > latestTradeId) {
                                                latestTradeId = notification.tradeId
                                            }

                                            if (tradeList == null) {
                                                tradeList = LinkedList()
                                            }

                                            tradeList!!.add(BareTrade(notification.amount, notification.price, notification.feeMultiplier))
                                        }
                                    }
                                    is LimitOrderCreated -> run {
                                        if (notification.orderId == currOrderId) {
                                            orderCreateReceived = notification
                                        }
                                    }
                                    is OrderUpdate -> run {
                                        if (notification.orderId == currOrderId && notification.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                            orderCancelReceived = notification
                                        }
                                    }
                                    else -> run {
                                        // ignore other events
                                    }
                                }
                            }

                            if (tradeList != null && !tradeList!!.isEmpty()) {
                                scheduler.addTrades(tradeList!!)
                                thisJob?.cancel()
                            }

                            if (orderCreateReceived != null) orderCreateConfirmed.get()?.complete(Unit)
                            if (orderCancelReceived != null) orderCancelConfirmed.get()?.complete(orderCancelReceived!!.orderType)
                        }
                    }
                }
            }

            try {
                var closeAllError: Throwable?

                while (isActive) {
                    closeAllError = null

                    logger.debug { "Start market maker trade process" }

                    try {
                        coroutineScope {
                            launch(start = CoroutineStart.UNDISPATCHED) {
                                logger.debug { "Start connection monitoring" }

                                poloniexApi.connection.collect { connected ->
                                    if (!connected) throw DisconnectedException
                                }
                            }

                            withContext(NonCancellable) {
                                commonStateMutex.withLock {
                                    if (thisJob?.isCancelled == true) return@withContext

                                    val placeOrderResult = placeOrder(scheduler.fromAmount)

                                    logger.debug { "Order has been placed" }

                                    currOrderId = placeOrderResult._1
                                    prevPrice = placeOrderResult._2
                                    prevQuoteAmount = placeOrderResult._3

                                    orderCreateConfirmed.set(CompletableDeferred())
                                    orderCancelConfirmed.set(CompletableDeferred())
                                }
                            }

                            orderCreateConfirmed.get()?.await()

                            var prevFromAmount = scheduler.fromAmount
                            val timeout = Duration.ofSeconds(4)
                            val timeoutMillis = timeout.toMillis()
                            var bookUpdateTime = System.currentTimeMillis()
                            var bookChangeCounter = 0

                            logger.debug { "Start market maker movement process" }

                            orderBook.returnLastIfNoValueWithinSpecifiedTime(timeout).conflate().collect { book ->
                                try {
                                    withContext(NonCancellable) {
                                        commonStateMutex.withLock {
                                            if (thisJob?.isCancelled == true) return@withContext

                                            bookChangeCounter = if (bookChangeCounter >= 10) 0 else bookChangeCounter + 1
                                            val currFromAmount = scheduler.fromAmount
                                            val forceMove = prevFromAmount.compareTo(currFromAmount) != 0
                                            val now = System.currentTimeMillis()
                                            val fixPriceGaps = now - bookUpdateTime >= timeoutMillis || bookChangeCounter == 0
                                            bookUpdateTime = now
                                            prevFromAmount = currFromAmount

                                            val (orderId, price, quoteAmount) = moveOrder(
                                                book,
                                                currOrderId!!,
                                                prevPrice!!,
                                                prevQuoteAmount!!,
                                                currFromAmount,
                                                forceMove,
                                                fixPriceGaps
                                            ) ?: return@withContext

                                            logger.debug { "Moved order (q = $quoteAmount, p = $price)" }

                                            prevOrderId = currOrderId
                                            currOrderId = orderId
                                            prevPrice = price
                                            prevQuoteAmount = quoteAmount

                                            orderCreateConfirmed.set(CompletableDeferred())
                                            orderCancelConfirmed.set(CompletableDeferred())
                                        }
                                    }
                                } catch (e: CancellationException) {
                                    throw e
                                } catch (e: Throwable) {
                                    when (e) {
                                        is OrderCompletedOrNotExistException,
                                        is InvalidOrderNumberException,
                                        is NotEnoughCryptoException,
                                        is AmountMustBeAtLeastException,
                                        is TotalMustBeAtLeastException,
                                        is RateMustBeLessThanException -> run {
                                            logger.debug("${e.message}; fromAmount: ${scheduler.fromAmount};")
                                            delay(60000)
                                            throw e
                                        }
                                        else -> throw e
                                    }
                                }

                                orderCreateConfirmed.get()?.await()
                            }
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: DisconnectedException) {
                        logger.debug { "Disconnected from Poloniex" }

                        withContext(NonCancellable) {
                            poloniexApi.connection.filter { it }.first()
                            logger.debug { "Connected to Poloniex" }
                        }
                    } catch (e: Throwable) {
                        logger.debug("${e.message}; fromAmount: ${scheduler.fromAmount};")
                        closeAllError = e
                    } finally {
                        withContext(NonCancellable) {
                            try {
                                if (currOrderId == null) {
                                    logger.debug { "Don't have any opened orders" }
                                    return@withContext
                                }

                                val cancelConfirmedDef = orderCancelConfirmed.get()
                                val cancelConfirmed = cancelConfirmedDef?.isCompleted ?: true
                                if (!cancelConfirmed) cancelOrder(currOrderId!!)
                                val cancelConfirmTimeout = withTimeoutOrNull(Duration.ofMinutes(1).toMillis()) {
                                    logger.debug { "Waiting for order cancel confirmation..." }
                                    cancelConfirmedDef?.await()
                                    logger.debug { "Order cancel confirmation event has been received." }
                                }

                                if (cancelConfirmTimeout == null) {
                                    logger.warn { "Cancel event has not been received within specified timeout" }
                                }

                                val processedMissedTrades = processMissedTrades(currOrderId!!, latestTradeId)

                                if (processedMissedTrades) {
                                    thisJob?.cancel()
                                }

                                currOrderId = null
                                prevOrderId = null
                            } finally {
                                if (closeAllError != null) {
                                    scheduler.unregisterAll(closeAllError)
                                    thisJob?.cancel()
                                }
                            }
                        }
                    }
                }
            } finally {
                withContext(NonCancellable) {
                    tradeMonitoringJob.cancelAndJoin()
                }
            }
        }

        private suspend fun placeOrder(fromAmountValue: BigDecimal): Tuple3<Long, Price, Amount> {
            while (true) {
                try {
                    val primaryBook: SubOrderBook
                    val secondaryBook: SubOrderBook
                    val moveToOnePoint: (Price) -> Price

                    val book = orderBook.first()

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
                        val newPrice = moveToOnePoint(topPricePrimary)
                        val topPriceSecondary = secondaryBook.headOption().map { it._1 }.orNull
                        if (topPriceSecondary != null && topPriceSecondary.compareTo(newPrice) == 0) {
                            topPricePrimary
                        } else {
                            newPrice
                        }
                    } ?: throw OrderBookEmptyException(orderType)

                    val quoteAmount = when (orderType) {
                        OrderType.Buy -> calcQuoteAmount(fromAmountValue, price)
                        OrderType.Sell -> fromAmountValue
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
                    throw e
                } catch (e: AmountMustBeAtLeastException) {
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
                } catch (e: ConnectException) {
                    delay(2000)
                    continue
                } catch (e: SocketException) {
                    delay(2000)
                    continue
                } catch (e: Throwable) {
                    delay(2000)
                    if (logger.isDebugEnabled) logger.error(e.message)
                    continue
                }
            }
        }

        private suspend fun moveOrder(
            book: OrderBookAbstract,
            lastOrderId: Long,
            myPrice: Price,
            myQuoteAmount: Amount,
            fromAmountValue: Amount,
            forceMove: Boolean,
            fixPriceGaps: Boolean = false
        ): Tuple3<Long, Price, Amount>? {
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

                        val (bookPrice1, bookQuoteAmount1) = primaryBook.headOption().orNull ?: throw OrderBookEmptyException(orderType)
                        val myPositionInBook = priceComparator.compare(bookPrice1, myPrice)

                        if (myPositionInBook == 1 || myPositionInBook == 0 && myQuoteAmount.compareTo(bookQuoteAmount1) != 0) {
                            // I am on second position
                            var price = moveToOnePoint(bookPrice1)
                            val ask = secondaryBook.headOption().map { it._1 }.orNull
                            if (ask != null && ask.compareTo(price) == 0) price = bookPrice1
                            price
                        } else {
                            // I am on first position
                            if (fixPriceGaps) {
                                val secondPrice = primaryBook.drop(1).headOption().map { moveToOnePoint(it._1) }.orNull
                                if (secondPrice != null && priceComparator.compare(myPrice, secondPrice) == 1) secondPrice else null
                            } else {
                                null
                            }
                        }
                    } ?: if (forceMove) myPrice else return null

                    val quoteAmount = when (orderType) {
                        OrderType.Buy -> calcQuoteAmount(fromAmountValue, newPrice)
                        OrderType.Sell -> fromAmountValue
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
                    delay(100)
                } catch (e: TransactionFailedException) {
                    logger.debug(e.originalMsg)
                    delay(500)
                } catch (e: InvalidOrderNumberException) {
                    throw e
                } catch (e: NotEnoughCryptoException) {
                    throw e
                } catch (e: AmountMustBeAtLeastException) {
                    throw e
                } catch (e: TotalMustBeAtLeastException) {
                    throw e
                } catch (e: RateMustBeLessThanException) {
                    throw e
                } catch (e: OrderBookEmptyException) {
                    logger.warn(e.message)
                    delay(1000)
                } catch (e: MaxOrdersExceededException) {
                    logger.warn(e.originalMsg)
                    delay(1500)
                } catch (e: UnknownHostException) {
                    delay(2000)
                } catch (e: IOException) {
                    delay(2000)
                } catch (e: ConnectException) {
                    delay(2000)
                } catch (e: SocketException) {
                    delay(2000)
                } catch (e: Throwable) {
                    delay(2000)
                    if (logger.isDebugEnabled) logger.error(e.message)
                }
            }
        }

        private suspend fun cancelOrder(orderId: Long) {
            while (true) {
                try {
                    poloniexApi.cancelOrder(orderId)
                    break
                } catch (e: OrderCompletedOrNotExistException) {
                    logger.debug(e.message)
                    cancelUncaughtOrders()
                    break
                } catch (e: UnknownHostException) {
                    delay(2000)
                } catch (e: IOException) {
                    delay(2000)
                } catch (e: ConnectException) {
                    delay(2000)
                } catch (e: SocketException) {
                    delay(2000)
                } catch (e: Throwable) {
                    logger.error(e.message)
                    delay(2000)
                }
            }

            logger.debug { "Order $orderId has been cancelled" }
        }

        private suspend fun cancelUncaughtOrders() {
            logger.debug("Trying to find and cancel uncaught orders")

            coroutineScope {
                forceGetOpenOrders(market)
                    .asSequence()
                    .filter { it.type == orderType }
                    .map { async { cancelOrder(it.orderId) } }
                    .toList()
                    .forEach { it.await() }
            }

            logger.debug("All uncaught orders have been processed")
        }

        private suspend fun forceGetOpenOrders(market: Market): List<OpenOrder> {
            while (true) {
                try {
                    return poloniexApi.openOrders(market)
                } catch (e: UnknownHostException) {
                    delay(2000)
                } catch (e: IOException) {
                    delay(2000)
                } catch (e: ConnectException) {
                    delay(2000)
                } catch (e: SocketException) {
                    delay(2000)
                } catch (e: Throwable) {
                    logger.error(e.message)
                    delay(2000)
                }
            }
        }

        private suspend fun recoverAfterPowerOff() {
            logger.debug { "Trying to recover after power off..." }
            val (orderId, tradeId) = transactionsDao.getLatestOrderAndTradeId(market, orderType) ?: run {
                logger.debug { "Recover after power off is not required." }
                return
            }
            val orderIsOpen = data.openOrders.first().getOrNull(orderId) != null
            if (orderIsOpen) cancelOrder(orderId)
            processMissedTrades(orderId, tradeId)
        }

        private suspend fun processMissedTrades(orderId: Long, latestTradeId: Long): Boolean {
            while (true) {
                try {
                    logger.debug { "Trying to process missed trades..." }

                    val tradeList = poloniexApi.orderTrades(orderId).asSequence()
                        .filter { it.tradeId > latestTradeId }
                        .map { trade -> BareTrade(trade.amount, trade.price, trade.feeMultiplier) }
                        .toList()

                    return if (tradeList.isNotEmpty()) {
                        logger.debug { "Processing missed trades $tradeList..." }
                        scheduler.addTrades(tradeList)
                        true
                    } else {
                        false
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    if (logger.isDebugEnabled) logger.debug(e.message)
                    delay(1000)
                }
            }
        }
    }

    private inner class DelayedTradeManager(private val scope: CoroutineScope) {
        private val processors = hashMapOf<Tuple2<Market, OrderType>, DelayedTradeProcessor>()
        private val mutex = Mutex()

        suspend fun get(market: Market, orderType: OrderType): DelayedTradeProcessor {
            mutex.withLock {
                val key = tuple(market, orderType)
                val processor = processors[key]

                if (processor != null) {
                    logger.debug { "Delayed Trade Processor already exists for ($market, $orderType)." }
                    return processor
                }

                logger.debug { "Creating new Delayed Trade Processor for ($market, $orderType)..." }

                val orderBook = data.getOrderBookFlowBy(market)
                val newProcessor = DelayedTradeProcessor(market, orderType, orderBook, scope + CoroutineName("DELAYED_TRADE_PROCESSOR_${market}_$orderType"))
                processors[key] = newProcessor

                val processorJob = newProcessor.start()

                scope.launch(Job() + CoroutineName("DELAYED_TRADE_MANAGER_${market}_$orderType"), CoroutineStart.UNDISPATCHED) {
                    withContext(NonCancellable) {
                        processorJob.join()
                        logger.debug { "Delayed Trade Processor has completed its job in ($market, $orderType) market." }

                        mutex.withLock {
                            processors.remove(key)
                        }

                        logger.debug { "Delayed Trade Processor for ($market, $orderType) market has been removed from processors list." }
                    }
                }

                return newProcessor
            }
        }
    }

    private class IntentManager {
        private val paths = mutableListOf<TransactionIntent>()
        private val mutex = Mutex()

        suspend fun get(markets: Array<TranIntentMarket>, marketIdx: Int): TransactionIntent? {
            return mutex.withLock {
                logger.debug { "Trying to find similar intent in path manager..." }

                val intent = paths.find { it.marketIdx == marketIdx && areEqual(it.markets, markets) }

                if (intent != null) {
                    logger.debug { "Intent has been found in path manager" }
                } else {
                    logger.debug { "Intent has not been found in path manager" }
                }

                intent
            }
        }

        suspend fun add(intent: TransactionIntent) {
            mutex.withLock {
                paths.add(intent)
                logger.debug { "Intent has been added to path manager" }
            }
        }

        suspend fun remove(intentId: UUID) {
            mutex.withLock {
                paths.removeIf { it.id == intentId }
                logger.debug { "Intent has been removed from path manager" }
            }
        }

        companion object {
            private val logger = KotlinLogging.logger {}

            private fun areEqual(markets0: Array<TranIntentMarket>, markets1: Array<TranIntentMarket>): Boolean {
                if (markets0.length() != markets1.length()) return false
                var i = 0
                while (i < markets0.length()) {
                    val equal = markets0[i].market == markets1[i].market
                        && markets0[i].orderSpeed == markets1[i].orderSpeed
                        && markets0[i].fromCurrencyType == markets1[i].fromCurrencyType
                    if (!equal) return false
                    i++
                }
                return true
            }
        }
    }
}
