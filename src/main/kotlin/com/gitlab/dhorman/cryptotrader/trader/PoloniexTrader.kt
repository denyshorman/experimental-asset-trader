package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.CurrencyType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.algo.MergeTradeAlgo
import com.gitlab.dhorman.cryptotrader.trader.algo.SplitTradeAlgo
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.core.PoloniexTradeAdjuster
import com.gitlab.dhorman.cryptotrader.trader.dao.BlacklistedMarketsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.SettingsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.UnfilledMarketsDao
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.util.defaultTran
import com.gitlab.dhorman.cryptotrader.util.first
import com.gitlab.dhorman.cryptotrader.util.repeatableReadTran
import io.vavr.Tuple2
import io.vavr.collection.Array
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.reactive.collect
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.transaction.ReactiveTransactionManager
import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.time.Duration
import java.util.*

typealias PathId = UUID

@Service
class PoloniexTrader(
    private val poloniexApi: ExtendedPoloniexApi,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator,
    private val tradeAdjuster: PoloniexTradeAdjuster,
    private val pathGenerator: PathGenerator,
    private val transactionsDao: TransactionsDao,
    private val unfilledMarketsDao: UnfilledMarketsDao,
    private val settingsDao: SettingsDao,
    private val blacklistedMarketsDao: BlacklistedMarketsDao,
    @Qualifier("pg_tran_manager") private val tranManager: ReactiveTransactionManager,
    private val clock: Clock
) {
    private val logger = KotlinLogging.logger {}
    private val intentManager = IntentManager()
    private val tranIntentMarketExtensions = TranIntentMarketExtensions(amountCalculator, poloniexApi)
    private val mergeAlgo = MergeTradeAlgo(amountCalculator, tradeAdjuster, tranIntentMarketExtensions)
    private val splitAlgo = SplitTradeAlgo(amountCalculator, tradeAdjuster, tranIntentMarketExtensions)

    private lateinit var tranIntentScope: CoroutineScope
    private lateinit var delayedTradeManager: DelayedTradeManager
    private lateinit var tranIntentFactory: TransactionIntent.Companion.TransactionIntentFactory

    val tranRequests = Channel<Array<TranIntentMarket>>(Channel.RENDEZVOUS)

    fun start(scope: CoroutineScope) = scope.launch(CoroutineName("PoloniexTraderStarter")) {
        logger.info("Start trading on Poloniex")

        tranIntentScope = CoroutineScope(Dispatchers.Default + SupervisorJob(coroutineContext[Job]))
        delayedTradeManager = DelayedTradeManager(tranIntentScope, splitAlgo, poloniexApi, amountCalculator, transactionsDao, clock)
        tranIntentFactory = TransactionIntent.Companion.TransactionIntentFactory(
            tranIntentScope,
            intentManager,
            tranIntentMarketExtensions,
            transactionsDao,
            poloniexApi,
            amountCalculator,
            unfilledMarketsDao,
            settingsDao,
            tranManager,
            mergeAlgo,
            splitAlgo,
            delayedTradeManager,
            pathGenerator,
            blacklistedMarketsDao,
            clock
        )

        collectRoundingLeftovers()
        startMonitoringForTranRequests()
        resumeSleepingTransactions()

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

                val (bestPath, profit, _) = pathGenerator
                    .findBest(requestedAmount, startCurrency, requestedAmount, settingsDao.getPrimaryCurrencies()) ?: return@collect

                logger.debug {
                    "Found an optimal path: ${bestPath.marketsTinyString()} using amount $requestedAmount with potential profit $profit"
                }

                startPathTransaction(bestPath.toTranIntentMarket(requestedAmount, startCurrency))
            }
        } finally {
            logger.debug { "Trying to cancel all Poloniex transactions..." }

            tranIntentScope.cancel()
        }
    }

    private suspend fun collectRoundingLeftovers() {
        coroutineScope {
            val allBalancesAsync = async {
                val allBalances = poloniexApi.balanceStream.first()
                allBalances.removeAll(settingsDao.getPrimaryCurrencies())
            }
            val activePathsAsync = async {
                transactionsDao.getActive().asSequence()
                    .map {
                        val markets = it._2
                        val idx = tranIntentMarketExtensions.partiallyCompletedMarketIndex(markets)!!
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

            unfilledMarketsDao.add(settingsDao.getPrimaryCurrencies().first(), BigDecimal.ZERO, roundingLeftovers)

            logger.debug { "Added rounding leftovers $roundingLeftovers to unfilled amount list " }
        }
    }

    private fun CoroutineScope.startMonitoringForTranRequests(): Job = this.launch {
        for (tranRequest in tranRequests) {
            startPathTransaction(tranRequest)
        }
    }

    private suspend fun resumeSleepingTransactions() {
        logger.debug { "Trying to resume sleeping transactions..." }

        val sleepingTransactions = transactionsDao.getActive()

        for ((id, markets) in sleepingTransactions) {
            val startMarketIdx = tranIntentMarketExtensions.partiallyCompletedMarketIndex(markets)!!
            val initAmount = tranIntentMarketExtensions.fromAmount(markets.first(), markets, 0)
            val targetAmount = tranIntentMarketExtensions.targetAmount(markets.last(), markets, markets.length() - 1)

            if (initAmount > targetAmount) {
                logger.debug { "Restored path $id is not profitable (${targetAmount - initAmount}). Trying to find a new path..." }

                val currMarket = markets[startMarketIdx] as TranIntentMarketPartiallyCompleted
                val fromCurrency = currMarket.fromCurrency
                val fromCurrencyAmount = currMarket.fromAmount
                tranIntentScope.launch {
                    var bestPath: Array<TranIntentMarket>?

                    while (true) {
                        bestPath = pathGenerator
                            .findBest(initAmount, fromCurrency, fromCurrencyAmount, settingsDao.getPrimaryCurrencies())?._1
                            ?.toTranIntentMarket(fromCurrencyAmount, fromCurrency)

                        if (bestPath != null) {
                            logger.debug { "Found optimal path ${tranIntentMarketExtensions.pathString(bestPath)} for $id" }

                            break
                        } else {
                            logger.debug { "Optimal path was not found for $id (init = $initAmount, current = $fromCurrencyAmount). Retrying..." }
                        }

                        delay(60000)
                    }

                    val changedMarkets = markets.concat(startMarketIdx, bestPath!!)
                    val newId = UUID.randomUUID()

                    withContext(NonCancellable) {
                        tranManager.defaultTran {
                            transactionsDao.addActive(newId, changedMarkets, startMarketIdx)
                            transactionsDao.deleteActive(id)
                        }
                    }

                    tranIntentFactory.create(newId, changedMarkets, startMarketIdx).start()
                }
            } else {
                tranIntentFactory.create(id, markets, startMarketIdx).start()
            }
        }

        logger.debug { "Sleeping transactions restored: ${sleepingTransactions.size}" }
    }

    private suspend fun startPathTransaction(markets: Array<TranIntentMarket>) {
        val id = UUID.randomUUID()
        val marketIdx = 0
        val fromCurrency = markets[marketIdx].fromCurrency
        val requestedAmount = (markets[marketIdx] as TranIntentMarketPartiallyCompleted).fromAmount

        val canStartTransaction = tranManager.repeatableReadTran {
            val (available, onOrders) = poloniexApi.balanceStream.first().getOrNull(fromCurrency) ?: return@repeatableReadTran false
            val (_, amountInUse) = transactionsDao.balanceInUse(fromCurrency) ?: tuple(fromCurrency, BigDecimal.ZERO)
            val reservedAmount = onOrders - amountInUse
            val availableAmount = available + if (reservedAmount >= BigDecimal.ZERO) BigDecimal.ZERO else reservedAmount

            if (availableAmount >= requestedAmount) {
                transactionsDao.addActive(id, markets, marketIdx)
                true
            } else {
                false
            }
        }

        if (canStartTransaction) {
            tranIntentFactory.create(id, markets, marketIdx).start()
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

        val fr = amountCalculator.fromAmountBuy(q, p, f)
        val tr = amountCalculator.targetAmountBuy(q, p, f)

        val fd = initCurrencyAmount - fr
        val td = currentCurrencyAmount - tr

        val updatedMarkets = Array.of(
            TranIntentMarketCompleted(
                Market(initCurrency, currentCurrency),
                OrderSpeed.Instant,
                CurrencyType.Base,
                listOfNotNull(
                    BareTrade(q, p, f),
                    if (fd.compareTo(BigDecimal.ZERO) != 0) tradeAdjuster.adjustFromAmount(fd) else null,
                    if (td.compareTo(BigDecimal.ZERO) != 0) tradeAdjuster.adjustTargetAmount(td, OrderType.Buy) else null
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

        val bestPath = pathGenerator
            .findBest(initCurrencyAmount, currentCurrency, currentCurrencyAmount, settingsDao.getPrimaryCurrencies())?._1
            ?.toTranIntentMarket(currentCurrencyAmount, currentCurrency)

        if (bestPath == null) {
            logger.debug { "Path not found for ${tranIntentMarketExtensions.pathString(updatedMarkets)}" }
            return
        }

        val changedMarkets = updatedMarkets.concat(activeMarketId, bestPath)
        val tranId = UUID.randomUUID()

        withContext(NonCancellable) {
            tranManager.repeatableReadTran {
                unfilledMarketsDao.remove(id)
                transactionsDao.addActive(tranId, changedMarkets, activeMarketId)
            }
        }

        tranIntentFactory.create(tranId, changedMarkets, activeMarketId).start()
    }

    suspend fun requestBalanceForTransaction(): Tuple2<Currency, Amount>? {
        logger.trace { "Requesting new balance for transaction" }

        val primaryCurrencies = settingsDao.getPrimaryCurrencies()
        val fixedAmount = settingsDao.getFixedAmount()

        val usedBalances = transactionsDao.balancesInUse(primaryCurrencies)
            .groupBy({ it._1 }, { it._2 })
            .mapValues { it.value.reduce { a, b -> a + b } }

        val allBalances = poloniexApi.balanceStream.first()

        val minAmount = settingsDao.getMinTradeAmount()

        val availableBalance = allBalances.toVavrStream()
            .filter { primaryCurrencies.contains(it._1) }
            .map { currencyAvailableAndOnOrders ->
                val (currency, availableAndOnOrders) = currencyAvailableAndOnOrders
                val (available, onOrders) = availableAndOnOrders
                val balanceInUse = usedBalances.getOrDefault(currency, BigDecimal.ZERO)
                val reservedAmount = onOrders - balanceInUse
                var availableAmount = (available
                    - fixedAmount.getOrDefault(currency, BigDecimal.ZERO)
                    + if (reservedAmount >= BigDecimal.ZERO) BigDecimal.ZERO else reservedAmount)
                if (availableAmount < BigDecimal.ZERO) availableAmount = BigDecimal.ZERO
                tuple(currency, availableAmount)
            }
            .filter { it._2 >= minAmount }
            .firstOrNull()

        logger.trace { "usedBalances: $usedBalances ; allBalances: $allBalances ; availableBalance: $availableBalance" }

        if (availableBalance == null) return null

        val (currency, amount) = availableBalance
        val allocatedAmount = if (amount < minAmount + minAmount) amount else minAmount
        return tuple(currency, allocatedAmount)
    }
}
