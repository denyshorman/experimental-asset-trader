package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.DelayedOrder
import com.gitlab.dhorman.cryptotrader.core.ExhaustivePath
import com.gitlab.dhorman.cryptotrader.core.InstantOrder
import com.gitlab.dhorman.cryptotrader.core.OrderSpeed
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPredicted
import io.vavr.collection.Array
import io.vavr.collection.List

class PathHelper(
    private val indicators: IndicatorStreams,
    private val transactionsDao: TransactionsDao,
    private val tranIntentMarketExtensions: TranIntentMarketExtensions
) {
    suspend fun findNewPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>,
        recommendedChainCount: Int? = null
    ): Array<TranIntentMarket>? {
        val bestPath = selectBestPath(initAmount, fromCurrency, fromAmount, endCurrencies/*, recommendedChainCount*/) ?: return null
        return prepareMarketsForIntent(bestPath)
    }

    suspend fun selectBestPath(
        initAmount: Amount,
        fromCurrency: Currency,
        fromAmount: Amount,
        endCurrencies: List<Currency>,
        recommendedChainCount: Int? = null
    ): ExhaustivePath? {
        val activeTransactionIds = transactionsDao.getActive().map { tranIntentMarketExtensions.id(it._2) }

        val allPaths = indicators.getPaths(fromCurrency, fromAmount, endCurrencies, fun(p): Boolean {
            val targetMarket = p.chain.lastOrNull() ?: return false
            // if (p.chain.asSequence().filter { it is InstantOrder }.count() > 0) return false
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

