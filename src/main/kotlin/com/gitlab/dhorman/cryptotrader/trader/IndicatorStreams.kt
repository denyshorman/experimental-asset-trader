package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.ExhaustivePath
import com.gitlab.dhorman.cryptotrader.core.FeeMultiplier
import com.gitlab.dhorman.cryptotrader.core.TradeStat
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.MarketId
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.ExhaustivePathOrdering
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsUtil
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.collection.TreeSet
import mu.KotlinLogging
import reactor.core.publisher.Flux
import reactor.core.publisher.ReplayProcessor
import java.util.function.Function

class IndicatorStreams(data: DataStreams) {
    private val logger = KotlinLogging.logger {}

    val pathsSettings: ReplayProcessor<PathsSettings> = run {
        val defaultSettings = PathsSettings(40.toBigDecimal(), List.of("USDT", "USDC"))
        ReplayProcessor.cacheLastOrDefault(defaultSettings)
    }

    val paths: Flux<TreeSet<ExhaustivePath>> = Flux.combineLatest(
        data.markets,
        data.orderBooks,
        data.tradesStat,
        data.fee,
        pathsSettings,
        Function<Array<Any>, Flux<TreeSet<ExhaustivePath>>> { data0 ->
            @Suppress("UNCHECKED_CAST")
            val marketInfoStringMap = (data0[0] as MarketData)._2

            @Suppress("UNCHECKED_CAST")
            val orderBooks = data0[1] as OrderBookDataMap

            @Suppress("UNCHECKED_CAST")
            val stats = data0[2] as Map<MarketId, Flux<TradeStat>>

            val fee = data0[3] as FeeMultiplier
            val settings = data0[4] as PathsSettings

            val pathsPermutations = PathsUtil.generateSimplePaths(marketInfoStringMap.keySet(), settings.currencies)

            if (logger.isDebugEnabled) logger.debug("Paths generated: ${pathsPermutations.iterator().map { it._2.size() }.sum()}")

            val pathsPermutationsDelta = PathsUtil.wrapPathsPermutationsToStream(
                pathsPermutations,
                orderBooks,
                stats,
                marketInfoStringMap,
                settings.initialAmount,
                fee
            )

            pathsPermutationsDelta.scan(TreeSet.empty(ExhaustivePathOrdering)) { state, delta -> state.add(delta) }
        })
        .switchMap { it }
        .share()
}
