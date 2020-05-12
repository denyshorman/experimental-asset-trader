package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.DelayedOrder
import com.gitlab.dhorman.cryptotrader.core.ExhaustivePath
import com.gitlab.dhorman.cryptotrader.core.InstantOrder
import com.gitlab.dhorman.cryptotrader.core.OrderSpeed
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.dao.BlacklistedMarketsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPredicted
import io.vavr.collection.Array
import io.vavr.collection.List

class PathHelper(
    private val indicators: Indicators,
    private val transactionsDao: TransactionsDao,
    private val tranIntentMarketExtensions: TranIntentMarketExtensions,
    private val blacklistedMarketsDao: BlacklistedMarketsDao
) {
    suspend fun findNewPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>
    ): Array<TranIntentMarket>? {
        val bestPath = selectBestPath(initAmount, fromCurrency, fromAmount, endCurrencies) ?: return null
        return prepareMarketsForIntent(bestPath)
    }

    suspend fun selectBestPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>
    ): ExhaustivePath? {
        val activeTransactionIds = transactionsDao.getActive().map { tranIntentMarketExtensions.id(it._2) }
        val blacklistedMarkets = blacklistedMarketsDao.getAll()

        val allPaths = indicators.getPaths(fromCurrency, fromAmount, endCurrencies, fun(p): Boolean {
            val targetMarket = p.chain.lastOrNull() ?: return false
            val hasBlacklistedMarkets = p.chain.any { blacklistedMarkets.contains(it.market) }
            return !hasBlacklistedMarkets && initAmount < targetMarket.toAmount && !activeTransactionIds.contains(p.id)
        }, Comparator { p0, p1 ->
            if (p0.id == p1.id) {
                0
            } else {
                p1.profitability.compareTo(p0.profitability)
            }
        })

        return allPaths.headOption().orNull
    }

    fun prepareMarketsForIntent(bestPath: ExhaustivePath): Array<TranIntentMarket> {
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

    fun updateMarketsWithBestPath(markets: Array<TranIntentMarket>, marketIdx: Int, bestPath: Array<TranIntentMarket>): Array<TranIntentMarket> {
        return markets.dropRight(markets.length() - marketIdx).appendAll(bestPath)
    }
}

