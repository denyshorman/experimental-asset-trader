package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.dao.*
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import io.vavr.Tuple2
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.toVavrStream
import io.vavr.kotlin.tuple
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import mu.KotlinLogging
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*

@Component
class PathGenerator(
    private val poloniexApi: ExtendedPoloniexApi,
    private val transactionsDao: TransactionsDao,
    private val unfilledMarketsDao: UnfilledMarketsDao,
    private val settingsDao: SettingsDao,
    private val blacklistedMarketsDao: BlacklistedMarketsDao,
    private val marketLimitsDao: MarketLimitsDao,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator
) {
    private val tranIntentMarketExtensions = TranIntentMarketExtensions(amountCalculator, poloniexApi)

    suspend fun generate(fromCurrency: Currency, fromCurrencyAmount: Amount, toCurrencies: Iterable<Currency>): Sequence<SimulatedPath> {
        val blacklistedMarkets = blacklistedMarketsDao.getAll()
        val markets = poloniexApi.orderBooksPollingStream.first()
            .toVavrStream()
            .filter { (market, orderBook) -> !orderBook.isFrozen && !blacklistedMarkets.contains(market) }
            .map { it._1 }

        return MarketPathGenerator(markets)
            .generateWithOrders(listOf(fromCurrency), toCurrencies)
            .asSequence()
            .flatMap { (_, paths) ->
                paths.map { path ->
                    val orderIntents = path.map { (speed, market) -> SimulatedPath.OrderIntent(market, speed) }
                    SimulatedPath(orderIntents.toVavrStream().toArray())
                }
            }
    }

    private suspend fun findPath(settings: PathSettings): PathWithMetrics? {
        val fee = poloniexApi.feeStream.first()
        val orderBooks = poloniexApi.orderBooksPollingStream.first()
        val tradeVolumeStat = poloniexApi.tradeVolumeStat.first()
        val baseCurrencyLimits = marketLimitsDao.getAllBaseCurrencyLimits()
        val primaryCurrencies = settingsDao.getPrimaryCurrencies()

        val fromCurrency: Currency
        val fromAmount: Amount
        val endCurrencies: Iterable<Currency>
        val pathId: PathId?

        val filterPath: suspend (SimulatedPath) -> Tuple2<PathWithMetrics?, PathFilterReturnReason>

        when (settings) {
            is BestPathSettings -> run {
                val initAmount = settings.initAmount
                fromAmount = settings.fromAmount
                fromCurrency = settings.fromCurrency
                endCurrencies = settings.endCurrencies
                pathId = settings.pathId

                filterPath = f@{ path ->
                    try {
                        val pathAmountPrediction = path.amounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
                        val canProceed = pathCanProceed(path, pathAmountPrediction, fromCurrency, baseCurrencyLimits)
                        if (!canProceed) return@f tuple(null, PathFilterReturnReason.NotFitLimits)
                        val profit = pathAmountPrediction.last()._2 - initAmount
                        if (profit <= BigDecimal.ZERO) return@f tuple(null, PathFilterReturnReason.NotProfitable)
                        val waitTime = path.waitTime(fromCurrency, tradeVolumeStat, pathAmountPrediction)
                        val profitability = calcProfitability(profit, waitTime)
                        return@f tuple(PathWithMetrics(path, profit, profitability), PathFilterReturnReason.PathFits)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        logger.warn { "Can't determine suitability of the path $path. ${e.message}" }
                        return@f tuple(null, PathFilterReturnReason.MetricCalculationError)
                    }
                }
            }
            is BetterPathSettings -> run {
                val inputPath = settings.path
                val initAmount = tranIntentMarketExtensions.fromAmount(inputPath.first(), inputPath, 0)
                val currentMarketIndex = tranIntentMarketExtensions.partiallyCompletedMarketIndex(inputPath) ?: return null
                val currentMarket = inputPath[currentMarketIndex] as TranIntentMarketPartiallyCompleted
                fromCurrency = currentMarket.fromCurrency
                fromAmount = currentMarket.fromAmount
                endCurrencies = settings.endCurrencies
                pathId = settings.pathId

                val currPathMetrics = try {
                    val simulatedPath = inputPath.toSimulatedPath(currentMarketIndex)
                    val amounts = simulatedPath.amounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
                    val profit = amounts.last()._2 - initAmount
                    val waitTime = simulatedPath.waitTime(fromCurrency, tradeVolumeStat, amounts)
                    val profitability = calcProfitability(profit, waitTime)
                    PathWithMetrics(simulatedPath, profit, profitability)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    logger.warn("Can't calculate metrics for current path: ${e.message}")
                    return null
                }

                val currSimulatedPathDelayedCount = currPathMetrics.path.marketSpeedCount(OrderSpeed.Delayed)
                val currPathRiskFree = currPathMetrics.path.isRiskFree(fromCurrency, primaryCurrencies)
                val definedPriceThreshold = settingsDao.getCheckPathPriceThreshold()

                filterPath = f@{ path ->
                    try {
                        val pathAmountPrediction = path.amounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
                        var canProceed = pathCanProceed(path, pathAmountPrediction, fromCurrency, baseCurrencyLimits)
                        if (!canProceed) return@f tuple(null, PathFilterReturnReason.NotFitLimits)
                        val profit = pathAmountPrediction.last()._2 - initAmount
                        if (profit <= BigDecimal.ZERO) return@f tuple(null, PathFilterReturnReason.NotProfitable)
                        val waitTime = path.waitTime(fromCurrency, tradeVolumeStat, pathAmountPrediction)
                        val profitability = calcProfitability(profit, waitTime)
                        if (profitability <= currPathMetrics.profitability) return@f tuple(null, PathFilterReturnReason.NotFitBusinessCondition)
                        val pathRiskFree = path.isRiskFree(fromCurrency, primaryCurrencies)

                        when {
                            currPathRiskFree && !pathRiskFree -> return@f tuple(null, PathFilterReturnReason.NotFitBusinessCondition)
                            pathRiskFree -> return@f tuple(PathWithMetrics(path, profit, profitability), PathFilterReturnReason.PathFits)
                        }

                        val currPathFirstIntent = currPathMetrics.path.orderIntents.first()
                        val pathFirstIntent = path.orderIntents.first()

                        val pathSpeedCount = path.marketSpeedCount(OrderSpeed.Delayed)
                        val pathFaster = pathSpeedCount <= currSimulatedPathDelayedCount

                        canProceed = pathFaster
                            ||
                            pathFaster
                            && currPathFirstIntent.market == pathFirstIntent.market
                            && currPathFirstIntent.orderSpeed.fasterOrEqual(pathFirstIntent.orderSpeed)
                            ||
                            if (currPathMetrics.profit.compareTo(BigDecimal.ZERO) == 0) {
                                false
                            } else {
                                val threshold = profit.divide(currPathMetrics.profit, 8, RoundingMode.HALF_EVEN) - BigDecimal.ONE
                                threshold > definedPriceThreshold
                            }

                        if (!canProceed) return@f tuple(null, PathFilterReturnReason.NotFitBusinessCondition)

                        return@f tuple(PathWithMetrics(path, profit, profitability), PathFilterReturnReason.PathFits)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        logger.warn { "Can't determine suitability of the path $path. ${e.message}" }
                        return@f tuple(null, PathFilterReturnReason.MetricCalculationError)
                    }
                }
            }
        }

        return coroutineScope {
            val activeTransactionsFuture = async {
                transactionsDao.getActive().map { (pathId, markets) ->
                    val path = markets.toVavrStream().dropWhile { it is TranIntentMarketCompleted }.toList()
                    tuple(pathId, path)
                }
            }

            val unfilledCurrenciesFuture = async {
                unfilledMarketsDao.getAllCurrenciesWithInitAmountMoreOrEqual(settingsDao.getUnfilledInitAmountThreshold())
            }

            val activeTransactions = activeTransactionsFuture.await()
            val unfilledCurrencies = unfilledCurrenciesFuture.await()

            val pathCurrenciesInvolved = HashSet<Currency>()
            activeTransactions.forEach { (id, path) ->
                path.forEach { intent ->
                    if (pathId != id) {
                        pathCurrenciesInvolved.add(intent.fromCurrency)
                        pathCurrenciesInvolved.add(intent.targetCurrency)
                    }
                }
            }

            val currenciesToBeInvolved = LinkedList<UnfilledData>()
            unfilledCurrencies.forEach {
                if (!pathCurrenciesInvolved.contains(it.currency)) {
                    currenciesToBeInvolved.add(it)
                }
            }

            fun SimulatedPath.exists(): Boolean {
                for ((_, transaction) in activeTransactions) {
                    if (transaction.length() != this.orderIntents.length()) {
                        continue
                    }

                    val allMatched = transaction.zip(this.orderIntents).all { (tranMarket, intentMarket) ->
                        tranMarket.market == intentMarket.market && tranMarket.orderSpeed == intentMarket.orderSpeed
                    }

                    if (allMatched) {
                        return true
                    }
                }
                return false
            }

            val comparator = Comparator<PathWithMetrics> { data0, data1 ->
                val riskFree0 = data0.path.isRiskFree(fromCurrency, primaryCurrencies)
                val riskFree1 = data1.path.isRiskFree(fromCurrency, primaryCurrencies)

                if (riskFree0 && !riskFree1) {
                    return@Comparator 1
                } else if (!riskFree0 && riskFree1) {
                    return@Comparator -1
                }

                if (!currenciesToBeInvolved.isEmpty()) {
                    var amount0 = BigDecimal.ZERO
                    var amount1 = BigDecimal.ZERO
                    var waitTime0 = 0
                    var waitTime1 = 0
                    for ((_, amount, currency, _) in currenciesToBeInvolved) {
                        var i = 1
                        for (orderIntent in data0.path.orderIntents) {
                            if (orderIntent.market.contains(currency)) {
                                amount0 += amount
                                waitTime0 += i
                                break
                            }
                            if (orderIntent.orderSpeed == OrderSpeed.Delayed) i++
                        }
                        i = 1
                        for (orderIntent in data1.path.orderIntents) {
                            if (orderIntent.market.contains(currency)) {
                                amount1 += amount
                                waitTime1 += i
                                break
                            }
                            if (orderIntent.orderSpeed == OrderSpeed.Delayed) i++
                        }
                    }

                    when {
                        waitTime0 == 0 && waitTime1 == 0 -> {
                        }
                        waitTime0 == 0 && waitTime1 != 0 -> return@Comparator 1
                        waitTime0 != 0 && waitTime1 == 0 -> return@Comparator -1
                        else -> return@Comparator (amount0.toDouble() / waitTime0).compareTo(amount1.toDouble() / waitTime1)
                    }
                }
                val profitabilityComp = data0.profitability.compareTo(data1.profitability)
                if (profitabilityComp == 0) {
                    data0.profit.compareTo(data1.profit)
                } else {
                    profitabilityComp
                }
            }

            val activeTranSimulatedPaths = LinkedList<PathWithMetrics>()

            var selectedPath: PathWithMetrics? = null

            val allPaths = generate(fromCurrency, fromAmount, endCurrencies)
            val pathFilterReasonsCount = EnumMap<PathFilterReturnReason, Long>(PathFilterReturnReason::class.java)

            allPaths.asFlow().collect { simulatedPath ->
                val (pathMetrics, reason) = filterPath(simulatedPath)
                pathFilterReasonsCount[reason] = pathFilterReasonsCount[reason] ?: 0L + 1L
                if (pathMetrics == null) return@collect
                if (pathMetrics.path.exists()) {
                    activeTranSimulatedPaths.add(pathMetrics)
                    return@collect
                }
                if (selectedPath == null || comparator.compare(selectedPath, pathMetrics) == -1) {
                    selectedPath = pathMetrics
                }
            }

            if (selectedPath == null && activeTranSimulatedPaths.isEmpty()) {
                if (
                    pathFilterReasonsCount[PathFilterReturnReason.NotFitLimits] ?: 0L > 0L
                    && pathFilterReasonsCount[PathFilterReturnReason.NotProfitable] ?: 0L == 0L
                    && pathFilterReasonsCount[PathFilterReturnReason.MetricCalculationError] ?: 0L == 0L
                    && pathFilterReasonsCount[PathFilterReturnReason.NotFitBusinessCondition] ?: 0L == 0L
                ) {
                    throw PathCantBeFoundException
                }

                return@coroutineScope null
            }

            for (value in activeTranSimulatedPaths) {
                if (selectedPath == null || comparator.compare(selectedPath, value) == -1) {
                    selectedPath = value
                }
            }

            selectedPath
        }
    }

    suspend fun findPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: Iterable<Currency>,
        pathId: PathId? = null
    ): PathWithMetrics? {
        return findPath(BestPathSettings(initAmount, fromCurrency, fromAmount, endCurrencies, pathId))
    }

    suspend fun findPath(
        path: io.vavr.collection.Array<TranIntentMarket>,
        endCurrencies: Iterable<Currency>,
        pathId: PathId? = null
    ): PathWithMetrics? {
        return findPath(BetterPathSettings(path, endCurrencies, pathId))
    }

    data class PathWithMetrics(
        val path: SimulatedPath,
        val profit: Amount,
        val profitability: Amount
    )

    private enum class PathFilterReturnReason {
        PathFits,
        NotFitLimits,
        NotProfitable,
        MetricCalculationError,
        NotFitBusinessCondition,
    }
}

private val logger = KotlinLogging.logger {}

object PathCantBeFoundException : Throwable("", null, true, false)

private sealed class PathSettings

private data class BestPathSettings(
    val initAmount: Amount,
    val fromCurrency: Currency,
    val fromAmount: Amount,
    val endCurrencies: Iterable<Currency>,
    val pathId: PathId? = null
) : PathSettings()

private data class BetterPathSettings(
    val path: io.vavr.collection.Array<TranIntentMarket>,
    val endCurrencies: Iterable<Currency>,
    val pathId: PathId? = null
) : PathSettings()

fun io.vavr.collection.Array<TranIntentMarket>.toSimulatedPath(partiallyCompletedMarketIndex: Int): SimulatedPath {
    val orderIntents = this.toVavrStream()
        .drop(partiallyCompletedMarketIndex)
        .map { SimulatedPath.OrderIntent(it.market, it.orderSpeed) }
        .toArray()

    return SimulatedPath(orderIntents)
}

private fun pathCanProceed(
    path: SimulatedPath,
    pathAmountPrediction: Array<Tuple2<Amount, Amount>>,
    fromCurrency: Currency,
    baseCurrencyLimits: Map<Currency, Amount>
): Boolean {
    var currency = fromCurrency
    val amountsIterator = pathAmountPrediction.iterator()
    for (orderIntent in path.orderIntents) {
        val baseCurrencyLimit = baseCurrencyLimits[orderIntent.market.baseCurrency]
        if (baseCurrencyLimit != null) {
            val baseCurrencyAmount = when (currency) {
                orderIntent.market.baseCurrency -> amountsIterator.next()._1
                orderIntent.market.quoteCurrency -> amountsIterator.next()._2
                else -> {
                    logger.warn("Currency $currency does not exist in market ${orderIntent.market}")
                    return true
                }
            }

            if (baseCurrencyAmount < baseCurrencyLimit) return false
        }
        currency = orderIntent.market.other(currency) ?: run {
            logger.warn("Currency $currency does not exist in market ${orderIntent.market}")
            return true
        }
    }
    return true
}

fun calcProfitability(profit: BigDecimal, waitTime: BigDecimal): BigDecimal {
    return profit.divide(waitTime, 12, RoundingMode.HALF_EVEN)
}

private fun OrderSpeed.fasterOrEqual(b: OrderSpeed): Boolean {
    return when (this) {
        OrderSpeed.Instant -> true
        OrderSpeed.Delayed -> when (b) {
            OrderSpeed.Instant -> false
            OrderSpeed.Delayed -> true
        }
    }
}
