package com.gitlab.dhorman.cryptotrader.trader.indicator.paths

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.MarketId
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.MarketStringMap
import com.gitlab.dhorman.cryptotrader.trader.OrderBookDataMap
import io.vavr.Tuple2
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.collection.Set
import io.vavr.collection.Traversable
import io.vavr.kotlin.*
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.util.*

object PathsUtil {
    fun generateSimplePaths(
        markets: Traversable<Market>,
        currencies: Traversable<Currency>
    ): Map<Tuple2<Currency, Currency>, Set<List<Tuple2<PathOrderType, Market>>>> {
        return MarketPathGenerator(markets).generateAllPermutationsWithOrders(currencies)
    }

    fun wrapPathsPermutationsToStream(
        pathsPermutations: Map<Tuple2<Currency, Currency>, Set<List<Tuple2<PathOrderType, Market>>>>,
        orderBooks: OrderBookDataMap,
        stats: Map<MarketId, Flux<TradeStat>>,
        marketInfoStringMap: MarketStringMap,
        initialAmount: Amount,
        fee: FeeMultiplier
    ): Flux<ExhaustivePath> {
        val pathsIterable =
            pathsPermutations.toVavrStream().flatMap { (targetPath, paths) ->
                paths.toVavrStream().map { path ->
                    val dependencies = LinkedList<Flux<Any>>()

                    for ((tpe, market) in path) {
                        val marketId = marketInfoStringMap.get(market).get()
                        val orderBook = orderBooks.get(marketId).get().map { it.book as Any }.onBackpressureLatest()
                        dependencies += orderBook

                        when (tpe) {
                            PathOrderType.Delayed -> run {
                                val stat = stats.get(marketId).get().map { it as Any }.onBackpressureLatest()
                                dependencies += stat
                            }
                            PathOrderType.Instant -> run { /*ignore*/ }
                        }
                    }

                    val dependenciesStream = Flux.combineLatest(dependencies, 1) { it }

                    val exhaustivePath = dependenciesStream
                        .onBackpressureLatest()
                        .publishOn(Schedulers.parallel(), 1)
                        .map { booksStats -> map(targetPath, initialAmount, fee, path, booksStats).option() }
                        .filter { it.isDefined }
                        .map { it.get() }

                    exhaustivePath
                }
            }

        return Flux.empty<Flux<ExhaustivePath>>()
            .startWith(pathsIterable)
            .flatMap({ it.onBackpressureLatest() }, pathsIterable.size(), 1)
    }

    fun map(
        targetPath: TargetPath,
        startAmount: Amount,
        fee: FeeMultiplier,
        path: List<Tuple2<PathOrderType, Market>>,
        booksStats: Array<Any>
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
            OrderType.Buy -> stat.buy
            OrderType.Sell -> stat.sell
        }
    }
}