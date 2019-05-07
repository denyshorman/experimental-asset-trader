package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.ExhaustivePath
import com.gitlab.dhorman.cryptotrader.core.FeeMultiplier
import com.gitlab.dhorman.cryptotrader.core.MarketPathGenerator
import com.gitlab.dhorman.cryptotrader.core.TradeStat
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.MarketId
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.ExhaustivePathOrdering
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsUtil
import io.vavr.collection.Map
import io.vavr.collection.Traversable
import io.vavr.collection.TreeSet
import io.vavr.kotlin.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitSingle
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
class IndicatorStreams(private val data: DataStreams) {
    private val logger = KotlinLogging.logger {}

    fun getPaths(settings: PathsSettings): Flux<TreeSet<ExhaustivePath>> {
        return Flux.combineLatest(
            data.markets,
            data.orderBooks,
            data.tradesStat,
            data.fee,
            identity()
        )
            .onBackpressureLatest()
            .publishOn(Schedulers.elastic(), 1)
            .map { data0 ->
                @Suppress("UNCHECKED_CAST")
                val marketInfoStringMap = (data0[0] as MarketData)._2

                val fee = data0[3] as FeeMultiplier

                val pathsPermutations = PathsUtil.generateSimplePaths(marketInfoStringMap.keySet(), settings.currencies)

                val uniqueMarkets = PathsUtil.uniqueMarkets(pathsPermutations).map { marketInfoStringMap[it].get() }

                @Suppress("UNCHECKED_CAST")
                val orderBooks = (data0[1] as OrderBookDataMap)
                    .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
                    .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }

                @Suppress("UNCHECKED_CAST")
                val stats = (data0[2] as Map<MarketId, Flux<TradeStat>>)
                    .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
                    .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }

                if (logger.isDebugEnabled) logger.debug("Paths generated: ${pathsPermutations.iterator().map { it._2.size() }.sum()}")

                Flux.interval(Duration.ofSeconds(settings.recalculatePeriodSec))
                    .startWith(0)
                    .onBackpressureDrop()
                    .limitRate(1)
                    .switchMap({
                        val booksMap = Flux.fromIterable(orderBooks)
                            .flatMap(identity(), orderBooks.length(), 1)
                            .collectMap({ it._1 }, { it._2 })
                            .map { it.toVavrMap() }


                        val statsMap = Flux.fromIterable(stats)
                            .flatMap(identity(), stats.length(), 1)
                            .collectMap({ it._1 }, { it._2 })
                            .map { it.toVavrMap() }

                        Mono.zip(booksMap, statsMap) { b, s -> tuple(b, s) }
                    }, 1)
                    .onBackpressureLatest()
                    .publishOn(Schedulers.elastic(), 1)
                    .scan(TreeSet.empty(ExhaustivePathOrdering)) { _, bookStatDelta ->
                        val (orderBooks0, stats0) = bookStatDelta
                        var sortedPaths = TreeSet.empty(ExhaustivePathOrdering)
                        val exhaustivePaths = PathsUtil.map(
                            pathsPermutations,
                            orderBooks0,
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
            .switchMap(identity(), Int.MAX_VALUE)
            .share()
    }

    suspend fun getPaths(
        fromCurrency: Currency,
        fromCurrencyAmount: Amount,
        toCurrencies: Traversable<Currency>,
        pathFilter: (ExhaustivePath) -> Boolean,
        pathComparator: Comparator<ExhaustivePath>
    ): TreeSet<ExhaustivePath> {
        val markets = data.markets.awaitFirst()._2
        val fee = data.fee.awaitFirst()
        val paths = withContext(Dispatchers.IO) {
            MarketPathGenerator(markets.keySet())
                .generateWithOrders(list(fromCurrency), toCurrencies)
        }
        val uniqueMarkets = PathsUtil.uniqueMarkets(paths).map { markets[it].get() }

        val orderBooks = data.orderBooks.awaitFirst()
            .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
            .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }

        val stats = data.tradesStat.awaitFirst()
            .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
            .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }

        val booksMap = Flux.fromIterable(orderBooks)
            .flatMap(identity(), orderBooks.length(), 1)
            .collectMap({ it._1 }, { it._2 })
            .map { it.toVavrMap() }
            .awaitSingle()

        val statsMap = Flux.fromIterable(stats)
            .flatMap(identity(), stats.length(), 1)
            .collectMap({ it._1 }, { it._2 })
            .map { it.toVavrMap() }
            .awaitSingle()

        var availablePaths = TreeSet.empty(pathComparator)

        withContext(Dispatchers.IO) {
            val exhaustivePaths = PathsUtil.map(
                paths,
                booksMap,
                statsMap,
                markets,
                fromCurrencyAmount,
                fee
            )

            for (exhaustivePath in exhaustivePaths) {
                if (exhaustivePath != null && pathFilter(exhaustivePath)) {
                    availablePaths = availablePaths.add(exhaustivePath)
                }
            }
        }

        return availablePaths
    }
}
