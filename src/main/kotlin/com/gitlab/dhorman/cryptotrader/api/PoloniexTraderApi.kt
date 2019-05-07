package com.gitlab.dhorman.cryptotrader.api

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Ticker
import com.gitlab.dhorman.cryptotrader.trader.PoloniexTrader
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import io.swagger.annotations.ApiOperation
import io.vavr.collection.List
import io.vavr.collection.Map
import org.springframework.http.MediaType
import org.springframework.messaging.handler.annotation.MessageMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toMono
import reactor.core.scheduler.Schedulers
import java.time.Duration

@RestController
@RequestMapping(value = ["/api/traders/poloniex"], produces = [MediaType.APPLICATION_JSON_UTF8_VALUE])
class PoloniexTraderApi(private val poloniexTrader: PoloniexTrader) {
    @ApiOperation(
        value = "Retrieve ticker snapshot",
        notes = "Use this resource to retrieve ticker snapshot"
    )
    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/tickers"])
    fun tickersSnapshot(): Mono<Map<Market, Ticker>> {
        return poloniexTrader.data.tickers.take(1).toMono()
    }

    @ApiOperation(
        value = "Retrieve balance snapshot",
        notes = "Use this resource to retrieve balance snapshot"
    )
    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/balances"])
    fun balancesSnapshot(): Mono<Map<Currency, Amount>> {
        return poloniexTrader.data.balances.take(1).toMono()
    }

    @MessageMapping("traders/poloniex/tickers")
    fun tickers() = run {
        poloniexTrader.data.tickers.sample(Duration.ofSeconds(1))
    }

    @MessageMapping("traders/poloniex/paths")
    fun paths(initAmount: Amount, currencies: List<Currency>) = run {
        poloniexTrader.indicators.getPaths(PathsSettings(initAmount, currencies))
            .sampleFirst(Duration.ofSeconds(30))
            .onBackpressureLatest()
            .flatMapSequential({
                Flux.fromIterable(it)
                    .buffer(250)
                    .subscribeOn(Schedulers.elastic())
            }, 1, 1)
    }
}