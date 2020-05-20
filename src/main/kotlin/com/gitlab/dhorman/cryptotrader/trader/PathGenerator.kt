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
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.collection.Queue
import io.vavr.kotlin.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withContext
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
    suspend fun generate(fromCurrency: Currency, fromCurrencyAmount: Amount, toCurrencies: Iterable<Currency>): List<SimulatedPath> {
        val blacklistedMarkets = blacklistedMarketsDao.getAll()
        val markets = poloniexApi.orderBooksPollingStream.first()
            .toVavrStream()
            .filter { (market, orderBook) -> !orderBook.isFrozen && !blacklistedMarkets.contains(market) }
            .map { it._1 }
            .toList()

        return withContext(Dispatchers.IO) {
            MarketPathGenerator(markets)
                .generateWithOrders(list(fromCurrency), toCurrencies.toVavrStream())
                .toVavrStream()
                .flatMap { (_, paths) ->
                    paths.toVavrStream().map { path ->
                        val orderIntents = path.map { (speed, market) ->
                            SimulatedPath.OrderIntent(market, speed)
                        }.toArray()
                        SimulatedPath(orderIntents)
                    }
                }
                .toList()
        }
    }

    suspend fun findBest(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>
    ): kotlin.collections.List<Tuple3<SimulatedPath, BigDecimal, BigDecimal>> {
        val allPaths = generate(fromCurrency, fromAmount, endCurrencies)
        val fee = poloniexApi.feeStream.first()
        val orderBooks = poloniexApi.orderBooksPollingStream.first()
        val tradeVolumeStat = poloniexApi.tradeVolumeStat.first()

        val filteredPaths = allPaths.asFlow()
            .simulatedPathWithProfit(initAmount, fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
            .filter { (_, profit) -> profit > BigDecimal.ZERO }
            .simulatedPathWithProfitWithProfitability(allPaths.size(), fromCurrency, fromAmount, fee, tradeVolumeStat, orderBooks, amountCalculator)
            .toList(LinkedList())

        return filteredPaths.sortedWith(Comparator { (_, profit0, profitability0), (_, profit1, profitability1) ->
            val profitabilityComp = profitability1.compareTo(profitability0)
            if (profitabilityComp == 0) {
                profit1.compareTo(profit0)
            } else {
                profitabilityComp
            }
        })
    }
}

private val logger = KotlinLogging.logger {}

fun Flow<SimulatedPath>.simulatedPathWithProfit(
    initAmount: Amount,
    fromCurrency: Currency,
    fromAmount: Amount,
    fee: FeeMultiplier,
    orderBooks: Map<Market, out OrderBookAbstract>,
    amountCalculator: AdjustedPoloniexBuySellAmountCalculator
): Flow<Tuple2<SimulatedPath, BigDecimal>> {
    return transform { path ->
        try {
            val targetAmount = path.targetAmount(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
            emit(tuple(path, targetAmount - initAmount))
        } catch (e: Throwable) {
            logger.warn { "Can't calculate targetAmount for $path. ${e.message}" }
        }
    }
}

fun Flow<Tuple2<SimulatedPath, BigDecimal>>.simulatedPathWithProfitWithProfitability(
    allPathsSize: Int,
    fromCurrency: Currency,
    fromAmount: Amount,
    fee: FeeMultiplier,
    tradeVolumeStat: Map<Market, Flow<Queue<TradeVolumeStat>>>,
    orderBooks: Map<Market, out OrderBookAbstract>,
    amountCalculator: AdjustedPoloniexBuySellAmountCalculator
): Flow<Tuple3<SimulatedPath, BigDecimal, BigDecimal>> {
    return flatMapMerge(allPathsSize) { (path, profit) ->
        flow {
            try {
                val waitTime = path.waitTime(fromCurrency, fromAmount, fee, orderBooks, tradeVolumeStat, amountCalculator)
                val profitability = profit.divide(waitTime, 16, RoundingMode.HALF_EVEN)
                emit(tuple(path, profit, profitability))
            } catch (e: Throwable) {
                logger.warn { "Can't calculate waitTime for $path. ${e.message}" }
            }
        }.flowOn(Dispatchers.Default)
    }
}

suspend fun kotlin.collections.List<Tuple3<SimulatedPath, BigDecimal, BigDecimal>>.findOne(
    transactionsDao: TransactionsDao
): Tuple3<SimulatedPath, BigDecimal, BigDecimal>? {
    if (this.isEmpty()) return null

    val activeTransactions = transactionsDao.getActive().map { (_, markets) ->
        markets.toVavrStream().dropWhile { it is TranIntentMarketCompleted }.toList()
    }

    var selectedPath: Tuple3<SimulatedPath, BigDecimal, BigDecimal>? = null

    for (value in this) {
        val (path, _, _) = value
        var matched = false

        for (transaction in activeTransactions) {
            if (transaction.length() != path.orderIntents.length()) {
                continue
            }

            val allMatched = transaction.zip(path.orderIntents).all { (tranMarket, intentMarket) ->
                tranMarket.market == intentMarket.market && tranMarket.orderSpeed == intentMarket.orderSpeed
            }

            if (allMatched) {
                matched = true
                break
            }
        }

        if (!matched) {
            selectedPath = value
            break
        }
    }

    return selectedPath ?: this.first()
}
