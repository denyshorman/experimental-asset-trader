package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.ExhaustivePathOrdering
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsUtil
import com.gitlab.dhorman.cryptotrader.trader.model.MarketData
import com.gitlab.dhorman.cryptotrader.trader.model.OrderBookDataMap
import com.gitlab.dhorman.cryptotrader.util.collectMap
import com.gitlab.dhorman.cryptotrader.util.flowFromMap
import io.vavr.collection.Map
import io.vavr.collection.Traversable
import io.vavr.collection.TreeSet
import io.vavr.kotlin.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asPublisher
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.*
import java.util.function.Function.identity

@Component
class IndicatorStreams(
    private val data: DataStreams,
    amountCalculator: AdjustedPoloniexBuySellAmountCalculator
) {
    private val logger = KotlinLogging.logger {}
    private val pathsUtil = PathsUtil(amountCalculator)

    fun getPathsIntensive(settings: PathsSettings): Flux<TreeSet<ExhaustivePath>> {
        return Flux.combineLatest(
            data.markets.asPublisher(),
            data.orderBooks.asPublisher(),
            data.tradesStat.asPublisher(),
            data.fee.asPublisher(),
            identity()
        )
            .onBackpressureLatest()
            .publishOn(Schedulers.elastic(), 1)
            .map { data0 ->
                @Suppress("UNCHECKED_CAST")
                val marketInfoStringMap = (data0[0] as MarketData)._2

                val fee = data0[3] as FeeMultiplier

                val pathsPermutations = pathsUtil.generateSimplePaths(marketInfoStringMap.keySet(), settings.currencies)

                val uniqueMarkets = pathsUtil.uniqueMarkets(pathsPermutations).map { marketInfoStringMap[it].get() }

                @Suppress("UNCHECKED_CAST")
                val orderBooks = (data0[1] as OrderBookDataMap)
                    .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
                    .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }

                @Suppress("UNCHECKED_CAST")
                val stats = (data0[2] as Map<MarketId, Flow<TradeStat>>)
                    .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
                    .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }

                if (logger.isDebugEnabled) logger.debug("Paths generated: ${pathsPermutations.iterator().map { it._2.size() }.sum()}")

                Flux.interval(Duration.ofSeconds(settings.recalculatePeriodSec))
                    .startWith(0)
                    .onBackpressureDrop()
                    .limitRate(1)
                    .switchMap({
                        val booksMap = Flux.fromIterable(orderBooks.map { it.asPublisher() })
                            .flatMap({ it }, orderBooks.length(), 1)
                            .collectMap({ it._1 }, { it._2 })
                            .map { it.toVavrMap() }


                        val statsMap = Flux.fromIterable(stats.map { it.asPublisher() })
                            .flatMap({ it }, stats.length(), 1)
                            .collectMap({ it._1 }, { it._2 })
                            .map { it.toVavrMap() }

                        Mono.zip(booksMap, statsMap) { b, s -> tuple(b, s) }
                    }, 1)
                    .onBackpressureLatest()
                    .publishOn(Schedulers.elastic(), 1)
                    .scan(TreeSet.empty(ExhaustivePathOrdering)) { _, bookStatDelta ->
                        val (orderBooks0, stats0) = bookStatDelta
                        var sortedPaths = TreeSet.empty(ExhaustivePathOrdering)
                        val exhaustivePaths = pathsUtil.map(
                            pathsPermutations,
                            orderBooks0.mapValues { it.book },
                            stats0,
                            marketInfoStringMap,
                            settings.initialAmount,
                            fee
                        )
                        for (exhaustivePath in exhaustivePaths) {
                            if (exhaustivePath != null) {
                                sortedPaths = sortedPaths.add(exhaustivePath)
                            }
                        }
                        sortedPaths
                    }
                    .skip(1)
            }
            .switchMap({ it }, Int.MAX_VALUE)
            .share()
    }

    fun getPathsPolling(settings: PathsSettings): Flux<TreeSet<ExhaustivePath>> {
        return Flux.combineLatest(
            data.markets.asPublisher(),
            data.orderBooksPolling.asPublisher(),
            data.tradesStat.asPublisher(),
            data.fee.asPublisher(),
            identity()
        )
            .onBackpressureLatest()
            .publishOn(Schedulers.elastic(), 1)
            .map { data0 ->
                val (marketIntMap, marketStringMap) = run {
                    @Suppress("UNCHECKED_CAST")
                    data0[0] as MarketData
                }

                @Suppress("UNCHECKED_CAST")
                val marketBookMap = data0[1] as Map<Market, OrderBookSnapshot>

                @Suppress("UNCHECKED_CAST")
                val statMap = data0[2] as Map<MarketId, Flow<TradeStat>>

                val fee = data0[3] as FeeMultiplier

                val pathsPermutations = pathsUtil.generateSimplePaths(marketBookMap.keySet(), settings.currencies)

                val uniqueMarkets = pathsUtil.uniqueMarkets(pathsPermutations)

                val orderBooks = marketBookMap.toVavrStream()
                    .filter { (market, _) -> uniqueMarkets.contains(market) && marketStringMap.containsKey(market) }
                    .map { (market, orderBook) ->
                        val marketId = marketStringMap.getOrNull(market)!!
                        val orderBookData = PriceAggregatedBook(orderBook.asks, orderBook.bids)
                        tuple(marketId, orderBookData)
                    }
                    .toMap { it }

                val stats = statMap.toVavrStream()
                    .filter { (marketId, _) ->
                        marketIntMap[marketId].map { market -> uniqueMarkets.contains(market) }.getOrElse(false)
                    }
                    .map { (marketId, tradeStatFlow) ->
                        tradeStatFlow.map { stat -> tuple(marketId, stat) }.take(1)
                    }
                    .toList()

                logger.debug { "Paths generated: ${pathsPermutations.iterator().map { it._2.size() }.sum()}" }

                Flux.interval(Duration.ofSeconds(settings.recalculatePeriodSec))
                    .startWith(0)
                    .onBackpressureDrop()
                    .limitRate(1)
                    .switchMap({
                        val booksMap = Mono.just(orderBooks)

                        val statsMap = Flux.fromIterable(stats.map { it.asPublisher() })
                            .flatMap({ it }, stats.length(), 1)
                            .collectMap({ it._1 }, { it._2 })
                            .map { it.toVavrMap() }

                        Mono.zip(booksMap, statsMap) { b, s -> tuple(b, s) }
                    }, 1)
                    .onBackpressureLatest()
                    .publishOn(Schedulers.elastic(), 1)
                    .scan(TreeSet.empty(ExhaustivePathOrdering)) { _, bookStatDelta ->
                        val (orderBooks0, stats0) = bookStatDelta
                        var sortedPaths = TreeSet.empty(ExhaustivePathOrdering)
                        val exhaustivePaths = pathsUtil.map(
                            pathsPermutations,
                            orderBooks0,
                            stats0,
                            marketStringMap,
                            settings.initialAmount,
                            fee
                        )
                        for (exhaustivePath in exhaustivePaths) {
                            if (exhaustivePath != null) {
                                sortedPaths = sortedPaths.add(exhaustivePath)
                            }
                        }
                        sortedPaths
                    }
                    .skip(1)
            }
            .switchMap({ it }, Int.MAX_VALUE)
            .share()
    }

    suspend fun getPaths(
        fromCurrency: Currency,
        fromCurrencyAmount: Amount,
        toCurrencies: Traversable<Currency>,
        pathFilter: (ExhaustivePath) -> Boolean,
        pathComparator: Comparator<ExhaustivePath>
    ): TreeSet<ExhaustivePath> = coroutineScope {
        val (marketIntMap, marketStringMap) = data.markets.first()
        val fee = data.fee.first()
        val orderBookMap = data.orderBooksPolling.first().filterValues { !it.isFrozen }
        val paths = withContext(Dispatchers.IO) {
            MarketPathGenerator(orderBookMap.keySet())
                .generateWithOrders(list(fromCurrency), toCurrencies)
        }
        val uniqueMarkets = pathsUtil.uniqueMarkets(paths)

        val booksMapDeferrable = async {
            orderBookMap.toVavrStream()
                .filter { (market, _) -> uniqueMarkets.contains(market) && marketStringMap.containsKey(market) }
                .map { (market, orderBook) ->
                    val marketId = marketStringMap.getOrNull(market)!!
                    val orderBookData = PriceAggregatedBook(orderBook.asks, orderBook.bids)
                    tuple(marketId, orderBookData)
                }
                .toMap { it }
        }

        val statsMapDeferrable = async {
            val stats = data.tradesStat.first()

            flowFromMap(stats)
                .filter { (marketId, _) ->
                    marketIntMap[marketId].map { market -> uniqueMarkets.contains(market) }.getOrElse(false)
                }
                .flatMapMerge(stats.length()) { (marketId, tradeStatFlow) ->
                    tradeStatFlow.map { stat -> tuple(marketId, stat) }.take(1)
                }
                .collectMap()
        }

        withContext(Dispatchers.IO) {
            var availablePaths = TreeSet.empty(pathComparator)

            val exhaustivePaths = pathsUtil.map(
                paths,
                booksMapDeferrable.await(),
                statsMapDeferrable.await(),
                marketStringMap,
                fromCurrencyAmount,
                fee
            )

            for (exhaustivePath in exhaustivePaths) {
                if (exhaustivePath != null && pathFilter(exhaustivePath)) {
                    availablePaths = availablePaths.add(exhaustivePath)
                }
            }

            availablePaths
        }
    }
}
