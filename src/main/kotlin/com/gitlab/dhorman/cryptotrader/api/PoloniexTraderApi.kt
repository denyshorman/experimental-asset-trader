package com.gitlab.dhorman.cryptotrader.api

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Ticker
import com.gitlab.dhorman.cryptotrader.trader.PoloniexTrader
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import io.swagger.annotations.ApiOperation
import io.vavr.Tuple2
import io.vavr.collection.List
import io.vavr.collection.Map
import io.vavr.kotlin.toVavrList
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.http.MediaType
import org.springframework.messaging.handler.annotation.MessageMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.time.Duration

@RestController
@RequestMapping(value = ["/api/traders/poloniex"], produces = [MediaType.APPLICATION_JSON_VALUE])
class PoloniexTraderApi(private val poloniexTrader: PoloniexTrader) {
    @ApiOperation(
        value = "Retrieve ticker snapshot",
        notes = "Use this resource to retrieve ticker snapshot"
    )
    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/tickers"])
    suspend fun tickersSnapshot(): Map<Market, Ticker> {
        return poloniexTrader.data.tickers.take(1).awaitSingle()
    }

    @ApiOperation(
        value = "Retrieve balance snapshot",
        notes = "Use this resource to retrieve balance snapshot"
    )
    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/balances"])
    suspend fun balancesSnapshot(): Map<Currency, Tuple2<Amount, Amount>> {
        return poloniexTrader.data.balances.take(1).awaitSingle()
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/paths"])
    suspend fun pathsSnapshot(@RequestParam initAmount: Amount, @RequestParam currencies: kotlin.collections.List<Currency>) =
        run {
            poloniexTrader.indicators.getPaths(PathsSettings(initAmount, currencies.toVavrList()))
                .sampleFirst(Duration.ofSeconds(30))
                .onBackpressureLatest()
                .flatMapSequential({
                    Flux.fromIterable(it)
                        .buffer(250)
                        .subscribeOn(Schedulers.elastic())
                }, 1, 1)
                .take(1)
                .awaitSingle()
        }

    //@MessageMapping("/tickers")
    @RequestMapping(method = [RequestMethod.GET], value = ["/tickers"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun tickers() = run {
        poloniexTrader.data.tickers.sample(Duration.ofSeconds(1))
    }

    //@MessageMapping("/paths")
    @RequestMapping(method = [RequestMethod.GET], value = ["/paths"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun paths(
        @RequestParam(required = true) initAmount: Amount,
        @RequestParam(required = true) currencies: kotlin.collections.List<Currency>
    ) = run {
        poloniexTrader.indicators.getPaths(PathsSettings(initAmount, currencies.toVavrList()))
            .sampleFirst(Duration.ofSeconds(30))
            .onBackpressureLatest()
            .flatMapSequential({
                Flux.fromIterable(it)
                    .buffer(250)
                    .subscribeOn(Schedulers.elastic())
            }, 1, 1)
    }
}