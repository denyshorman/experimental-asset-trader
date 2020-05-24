package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.TradeVolumeStat
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.dao.BlacklistedMarketsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.util.first
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.collection.Map
import io.vavr.collection.Queue
import io.vavr.kotlin.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*

@Component
class PathGenerator(
    private val poloniexApi: ExtendedPoloniexApi,
    private val blacklistedMarketsDao: BlacklistedMarketsDao,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator
) {
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

        return allPaths.asFlow()
            .simulatedPathWithAmounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
            .simulatedPathWithAmountsAndProfit()
            .filter { (_, _, profit) -> profit > BigDecimal.ZERO }
            .simulatedPathWithProfitAndProfitability(fromCurrency, tradeVolumeStat)
    }
}

private val logger = KotlinLogging.logger {}

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

fun Flow<Tuple2<SimulatedPath, Array<Tuple2<Amount, Amount>>>>.simulatedPathWithAmountsAndProfit(): Flow<Tuple3<SimulatedPath, Array<Tuple2<Amount, Amount>>, Amount>> {
    return map { (path, amounts) ->
        val profit = amounts.last()._2 - amounts.first()._1
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
                val profitability = profit.divide(waitTime, 16, RoundingMode.HALF_EVEN)
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
    transactionsDao: TransactionsDao
): Tuple3<SimulatedPath, BigDecimal, BigDecimal>? {
    val activeTransactions = transactionsDao.getActive().map { (_, markets) ->
        markets.toVavrStream().dropWhile { it is TranIntentMarketCompleted }.toList()
    }

    fun SimulatedPath.exists(): Boolean {
        for (transaction in activeTransactions) {
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

    val comparator = Comparator<Tuple3<SimulatedPath, BigDecimal, BigDecimal>> { (_, profit0, profitability0), (_, profit1, profitability1) ->
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
        if (selectedPath == null || comparator.compare(selectedPath, value) == 1) {
            selectedPath = value
        }
    }

    if (selectedPath == null && activeTranSimulatedPaths.isEmpty()) return null

    for (value in activeTranSimulatedPaths) {
        if (selectedPath == null || comparator.compare(selectedPath, value) == 1) {
            selectedPath = value
        }
    }

    return selectedPath
}
