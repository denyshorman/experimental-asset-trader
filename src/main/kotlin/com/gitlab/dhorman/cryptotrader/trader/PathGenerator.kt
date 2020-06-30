package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.TradeVolumeStat
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.dao.*
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.util.first
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.collection.Map
import io.vavr.collection.Queue
import io.vavr.kotlin.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
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

    suspend fun generateSimulatedPaths(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: Iterable<Currency>
    ): Flow<Tuple3<SimulatedPath, BigDecimal, BigDecimal>> {
        val allPaths = generate(fromCurrency, fromAmount, endCurrencies)
        val fee = poloniexApi.feeStream.first()
        val orderBooks = poloniexApi.orderBooksPollingStream.first()
        val tradeVolumeStat = poloniexApi.tradeVolumeStat.first()
        val baseCurrencyLimits = marketLimitsDao.getAllBaseCurrencyLimits()

        return allPaths.asFlow()
            .simulatedPathWithAmounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
            .filterAmountsByBaseCurrencyLimits(fromCurrency, baseCurrencyLimits)
            .simulatedPathWithAmountsAndProfit(initAmount)
            .filter { (_, _, profit) -> profit > BigDecimal.ZERO }
            .simulatedPathWithProfitAndProfitability(fromCurrency, tradeVolumeStat)
    }

    suspend fun findBest(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: Iterable<Currency>,
        pathId: PathId? = null
    ): Tuple3<SimulatedPath, BigDecimal, BigDecimal>? {
        return withContext(Dispatchers.IO) {
            generateSimulatedPaths(initAmount, fromCurrency, fromAmount, endCurrencies)
                .findOne(transactionsDao, unfilledMarketsDao, settingsDao.getUnfilledInitAmountThreshold(), pathId)
        }
    }

    suspend fun findBetter(
        path: io.vavr.collection.Array<TranIntentMarket>,
        endCurrencies: Iterable<Currency>,
        pathId: PathId? = null
    ): Tuple3<SimulatedPath, BigDecimal, BigDecimal>? {
        return withContext(Dispatchers.IO) {
            val initAmount = tranIntentMarketExtensions.fromAmount(path.first(), path, 0)
            val currentMarketIndex = tranIntentMarketExtensions.partiallyCompletedMarketIndex(path) ?: return@withContext null
            val currentMarket = path[currentMarketIndex] as TranIntentMarketPartiallyCompleted
            val fromCurrency = currentMarket.fromCurrency
            val fromAmount = currentMarket.fromAmount

            val fee = poloniexApi.feeStream.first()
            val orderBooks = poloniexApi.orderBooksPollingStream.first()
            val tradeVolumeStat = poloniexApi.tradeVolumeStat.first()

            val (currSimulatedPath, currProfit, currProfitability) = try {
                val simulatedPath = path.toSimulatedPath(currentMarketIndex)
                val amounts = simulatedPath.amounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
                val profit = amounts.last()._2 - initAmount
                val waitTime = simulatedPath.waitTime(fromCurrency, tradeVolumeStat, amounts)
                val profitability = calcProfitability(profit, waitTime)
                tuple(simulatedPath, profit, profitability)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                logger.warn("Can't calculate metrics for current path: ${e.message}")
                return@withContext null
            }

            val currSimulatedPathDelayedCount = currSimulatedPath.marketSpeedCount(OrderSpeed.Delayed)
            val definedPriceThreshold = settingsDao.getCheckPathPriceThreshold()
            val baseCurrencyLimits = marketLimitsDao.getAllBaseCurrencyLimits()

            generate(fromCurrency, fromAmount, endCurrencies).asFlow()
                .simulatedPathWithAmounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
                .filterAmountsByBaseCurrencyLimits(fromCurrency, baseCurrencyLimits)
                .simulatedPathWithAmountsAndProfit(initAmount)
                .filter { (_, _, profit) -> profit > currProfit }
                .simulatedPathWithProfitAndProfitability(fromCurrency, tradeVolumeStat)
                .filter { (_, _, profitability) -> profitability > currProfitability }
                .filter { (path, profit, _) ->
                    val currPathFirstIntent = currSimulatedPath.orderIntents.first()
                    val pathFirstIntent = path.orderIntents.first()

                    val pathSpeedCount = path.marketSpeedCount(OrderSpeed.Delayed)
                    val pathFaster = pathSpeedCount <= currSimulatedPathDelayedCount

                    pathFaster
                        ||
                        pathFaster
                        && currPathFirstIntent.market == pathFirstIntent.market
                        && currPathFirstIntent.orderSpeed.fasterOrEqual(pathFirstIntent.orderSpeed)
                        ||
                        if (currProfit.compareTo(BigDecimal.ZERO) == 0) {
                            false
                        } else {
                            val threshold = profit.divide(currProfit, 8, RoundingMode.HALF_EVEN) - BigDecimal.ONE
                            threshold > definedPriceThreshold
                        }
                }
                .findOne(transactionsDao, unfilledMarketsDao, settingsDao.getUnfilledInitAmountThreshold(), pathId)
        }
    }
}

private val logger = KotlinLogging.logger {}

fun io.vavr.collection.Array<TranIntentMarket>.toSimulatedPath(partiallyCompletedMarketIndex: Int): SimulatedPath {
    val orderIntents = this.toVavrStream()
        .drop(partiallyCompletedMarketIndex)
        .map { SimulatedPath.OrderIntent(it.market, it.orderSpeed) }
        .toArray()

    return SimulatedPath(orderIntents)
}

fun Flow<SimulatedPath>.simulatedPathWithAmounts(
    fromCurrency: Currency,
    fromAmount: Amount,
    fee: FeeMultiplier,
    orderBooks: Map<Market, out OrderBookAbstract>,
    amountCalculator: AdjustedPoloniexBuySellAmountCalculator
): Flow<Tuple2<SimulatedPath, Array<Tuple2<Amount, Amount>>>> {
    return transform { path ->
        try {
            val amounts = path.amounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
            emit(tuple(path, amounts))
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logger.warn { "Can't calculate targetAmount for $path. ${e.message}" }
        }
    }
}

fun Flow<Tuple2<SimulatedPath, Array<Tuple2<Amount, Amount>>>>.filterAmountsByBaseCurrencyLimits(
    fromCurrency: Currency,
    baseCurrencyLimits: kotlin.collections.Map<Currency, Amount>
): Flow<Tuple2<SimulatedPath, Array<Tuple2<Amount, Amount>>>> {
    return filter { (path, amounts) ->
        var currency = fromCurrency
        val amountsIterator = amounts.iterator()
        for (orderIntent in path.orderIntents) {
            val baseCurrencyLimit = baseCurrencyLimits[orderIntent.market.baseCurrency]
            if (baseCurrencyLimit != null) {
                val baseCurrencyAmount = when (currency) {
                    orderIntent.market.baseCurrency -> amountsIterator.next()._1
                    orderIntent.market.quoteCurrency -> amountsIterator.next()._2
                    else -> {
                        logger.warn("Currency $currency does not exist in market ${orderIntent.market}")
                        return@filter true
                    }
                }

                if (baseCurrencyAmount < baseCurrencyLimit) return@filter false
            }
            currency = orderIntent.market.other(currency) ?: run {
                logger.warn("Currency $currency does not exist in market ${orderIntent.market}")
                return@filter true
            }
        }
        true
    }
}

fun Flow<Tuple2<SimulatedPath, Array<Tuple2<Amount, Amount>>>>.simulatedPathWithAmountsAndProfit(initAmount: Amount): Flow<Tuple3<SimulatedPath, Array<Tuple2<Amount, Amount>>, Amount>> {
    return map { (path, amounts) ->
        val profit = amounts.last()._2 - initAmount
        tuple(path, amounts, profit)
    }
}

fun Flow<Tuple3<SimulatedPath, Array<Tuple2<Amount, Amount>>, Amount>>.simulatedPathWithProfitAndProfitability(
    fromCurrency: Currency,
    tradeVolumeStat: Map<Market, Flow<Queue<TradeVolumeStat>>>
): Flow<Tuple3<SimulatedPath, BigDecimal, BigDecimal>> {
    return flatMapMerge(Int.MAX_VALUE) { (path, amounts, profit) ->
        flow {
            try {
                val waitTime = path.waitTime(fromCurrency, tradeVolumeStat, amounts)
                val profitability = calcProfitability(profit, waitTime)
                emit(tuple(path, profit, profitability))
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                logger.warn { "Can't calculate waitTime for $path. ${e.message}" }
            }
        }
    }
}

suspend fun Flow<Tuple3<SimulatedPath, BigDecimal, BigDecimal>>.findOne(
    transactionsDao: TransactionsDao,
    unfilledMarketsDao: UnfilledMarketsDao,
    unfilledInitAmountThreshold: BigDecimal,
    pathId: PathId? = null
): Tuple3<SimulatedPath, BigDecimal, BigDecimal>? = coroutineScope {
    val activeTransactionsFuture = async {
        transactionsDao.getActive().map { (pathId, markets) ->
            val path = markets.toVavrStream().dropWhile { it is TranIntentMarketCompleted }.toList()
            tuple(pathId, path)
        }
    }

    val unfilledCurrenciesFuture = async {
        unfilledMarketsDao.getAllCurrenciesWithInitAmountMoreOrEqual(unfilledInitAmountThreshold)
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

    val currenciesToBeInvolved = LinkedList<Tuple2<Amount, Currency>>()
    unfilledCurrencies.forEach {
        val (_, unfilledCurrency) = it
        if (!pathCurrenciesInvolved.contains(unfilledCurrency)) {
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

    val comparator = Comparator<Tuple3<SimulatedPath, BigDecimal, BigDecimal>> { (path0, profit0, profitability0), (path1, profit1, profitability1) ->
        if (!currenciesToBeInvolved.isEmpty()) {
            var amount0 = BigDecimal.ZERO
            var amount1 = BigDecimal.ZERO
            var waitTime0 = 0
            var waitTime1 = 0
            for ((amount, currency) in currenciesToBeInvolved) {
                var i = 1
                for (orderIntent in path0.orderIntents) {
                    if (orderIntent.market.contains(currency)) {
                        amount0 += amount
                        waitTime0 += i
                        break
                    }
                    if (orderIntent.orderSpeed == OrderSpeed.Delayed) i++
                }
                i = 1
                for (orderIntent in path1.orderIntents) {
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
        val profitabilityComp = profitability0.compareTo(profitability1)
        if (profitabilityComp == 0) {
            profit0.compareTo(profit1)
        } else {
            profitabilityComp
        }
    }

    val activeTranSimulatedPaths = LinkedList<Tuple3<SimulatedPath, BigDecimal, BigDecimal>>()

    var selectedPath: Tuple3<SimulatedPath, BigDecimal, BigDecimal>? = null

    collect { value ->
        val (path, _, _) = value
        if (path.exists()) {
            activeTranSimulatedPaths.add(value)
            return@collect
        }
        if (selectedPath == null || comparator.compare(selectedPath, value) == -1) {
            selectedPath = value
        }
    }

    if (selectedPath == null && activeTranSimulatedPaths.isEmpty()) return@coroutineScope null

    for (value in activeTranSimulatedPaths) {
        if (selectedPath == null || comparator.compare(selectedPath, value) == -1) {
            selectedPath = value
        }
    }

    selectedPath
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
