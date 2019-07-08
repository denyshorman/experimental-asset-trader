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
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import io.vavr.collection.Traversable
import io.vavr.collection.TreeSet
import io.vavr.kotlin.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.reactive.flow.asPublisher
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

                val pathsPermutations = PathsUtil.generateSimplePaths(marketInfoStringMap.keySet(), settings.currencies)

                val uniqueMarkets = PathsUtil.uniqueMarkets(pathsPermutations).map { marketInfoStringMap[it].get() }

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
        val markets = data.markets.first()._2
        val fee = data.fee.first()
        val paths = withContext(Dispatchers.IO) {
            MarketPathGenerator(markets.keySet())
                .generateWithOrders(list(fromCurrency), toCurrencies)
        }
        val uniqueMarkets = PathsUtil.uniqueMarkets(paths).map { markets[it].get() }

        val booksMapDeferrable = async {
            var booksMap = HashMap.empty<MarketId, OrderBookData>()
            data.orderBooks.first()
                .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
                .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }
                .map { async { it.first() } }
                .forEach {
                    booksMap = booksMap.put(it.await())
                }
            booksMap
        }

        val statsMapDeferrable = async {
            var statsMap = HashMap.empty<MarketId, TradeStat>()
            data.tradesStat.first()
                .filter { marketId, _ -> uniqueMarkets.contains(marketId) }
                .map { kv -> kv._2.map { x -> tuple(kv._1, x) }.take(1) }
                .map { async { it.first() } }
                .forEach {
                    statsMap = statsMap.put(it.await())
                }
            statsMap
        }

        var availablePaths = TreeSet.empty(pathComparator)

        withContext(Dispatchers.IO) {
            val exhaustivePaths = PathsUtil.map(
                paths,
                booksMapDeferrable.await(),
                statsMapDeferrable.await(),
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

        availablePaths
    }
}
