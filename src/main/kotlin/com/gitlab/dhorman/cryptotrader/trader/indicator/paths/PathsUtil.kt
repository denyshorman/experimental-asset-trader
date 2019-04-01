package com.gitlab.dhorman.cryptotrader.trader.indicator.paths

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.MarketId
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.MarketStringMap
import com.gitlab.dhorman.cryptotrader.trader.OrderBookData
import io.vavr.Tuple2
import io.vavr.collection.*
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.collection.Set
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.toVavrList
import io.vavr.kotlin.toVavrStream
import java.math.BigDecimal
import java.util.*

object PathsUtil {
    fun generateSimplePaths(
        markets: Traversable<Market>,
        currencies: Traversable<Currency>
    ): Map<Tuple2<Currency, Currency>, Set<List<Tuple2<PathOrderType, Market>>>> {
        return MarketPathGenerator(markets).generateAllPermutationsWithOrders(currencies)
    }

    fun uniqueMarkets(pathsPermutations: Map<Tuple2<Currency, Currency>, Set<List<Tuple2<PathOrderType, Market>>>>): MutableSet<Market> {
        val marketSet = mutableSetOf<Market>()

        for ((_, paths) in pathsPermutations) {
            for (path in paths) {
                for ((_, market) in path) {
                    marketSet.add(market)
                }
            }
        }

        return marketSet
    }

    fun map(
        pathsPermutations: Map<Tuple2<Currency, Currency>, Set<List<Tuple2<PathOrderType, Market>>>>,
        orderBooks: Map<MarketId, OrderBookData>,
        stats: Map<MarketId, TradeStat>,
        marketInfoStringMap: MarketStringMap,
        initialAmount: Amount,
        fee: FeeMultiplier
    ): Stream<ExhaustivePath?> {
        return pathsPermutations.toVavrStream().flatMap { (targetPath, paths) ->
            paths.toVavrStream().map { path ->
                val booksStats = LinkedList<Any>()

                for ((tpe, market) in path) {
                    val marketId = marketInfoStringMap.get(market).get()
                    val orderBook = orderBooks[marketId].get().book
                    booksStats += orderBook

                    when (tpe) {
                        PathOrderType.Delayed -> run {
                            val stat = stats[marketId].get()
                            booksStats += stat
                        }
                        PathOrderType.Instant -> run { /*ignore*/ }
                    }
                }

                map(targetPath, initialAmount, fee, path, booksStats)
            }
        }
    }

    fun map(
        targetPath: TargetPath,
        startAmount: Amount,
        fee: FeeMultiplier,
        path: List<Tuple2<PathOrderType, Market>>,
        booksStats: LinkedList<Any>
    ): ExhaustivePath? {
        val chain = LinkedList<InstantDelayedOrder>()
        var targetCurrency = targetPath._1
        var fromAmount = startAmount

        var i = 0

        for ((tpe, market) in path) {
            targetCurrency = market.other(targetCurrency)!!
            val orderBook = booksStats[i] as OrderBookAbstract
            i += 1

            val order = when (tpe) {
                PathOrderType.Instant -> run {
                    mapInstantOrder(market, targetCurrency, fromAmount, fee.taker, orderBook)
                }
                PathOrderType.Delayed -> run {
                    val stat = booksStats[i] as TradeStat
                    i += 1
                    mapDelayedOrder(market, targetCurrency, fromAmount, fee.maker, orderBook, stat)
                }
            } ?: return null

            // TODO: Improve
            fromAmount = when (order) {
                is InstantOrder -> order.toAmount
                is DelayedOrder -> order.toAmount
            }

            chain += order
        }

        return ExhaustivePath(targetPath, chain.toVavrList()) // TODO: Improve interface inconsistency
    }

    private fun mapInstantOrder(
        market: Market,
        targetCurrency: Currency,
        fromAmount: Amount,
        takerFee: BigDecimal,
        orderBook: OrderBookAbstract
    ): InstantOrder? {
        return Orders.getInstantOrder(market, targetCurrency, fromAmount, takerFee, orderBook)
    }

    private fun mapDelayedOrder(
        market: Market,
        targetCurrency: Currency,
        fromAmount: Amount,
        makerFee: BigDecimal,
        orderBook: OrderBookAbstract,
        stat: TradeStat
    ): DelayedOrder? {
        val stat0 = statOrder(market, targetCurrency, stat)
        return Orders.getDelayedOrder(market, targetCurrency, fromAmount, makerFee, orderBook, stat0)
    }

    private fun statOrder(market: Market, targetCurrency: Currency, stat: TradeStat): TradeStatOrder {
        return when (market.orderType(targetCurrency)!!) {
            OrderType.Buy -> stat.sell
            OrderType.Sell -> stat.buy
        }
    }
}