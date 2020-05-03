package com.gitlab.dhorman.cryptotrader.api

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Ticker
import com.gitlab.dhorman.cryptotrader.trader.Indicators
import com.gitlab.dhorman.cryptotrader.trader.PoloniexTrader
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.indicator.paths.PathsSettings
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.Tuple4
import io.vavr.collection.Array
import io.vavr.collection.Map
import io.vavr.collection.TreeSet
import io.vavr.kotlin.*
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.sample
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.withTimeout
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.*

@RestController
@RequestMapping(value = ["/api/traders/poloniex"], produces = [MediaType.APPLICATION_JSON_VALUE])
class PoloniexTraderApi(
    private val poloniexTrader: PoloniexTrader,
    private val transactionsDao: TransactionsDao,
    private val poloniexApi: ExtendedPoloniexApi,
    private val indicators: Indicators,
    amountCalculator: AdjustedPoloniexBuySellAmountCalculator
) {
    private val tranIntentMarketExtensions = TranIntentMarketExtensions(amountCalculator, poloniexApi)

    // Example: USDT USDC 40.00 USDT_BTC1BTC_USDC0
    private val execTranBodyPattern =
        """^([a-z]+)\s+([a-z]+)\s+(\d+(?:\.\d+)?)\s+((?:[a-z]+_[a-z]+[01])+)$""".toRegex(RegexOption.IGNORE_CASE)

    private val execTranPathPattern =
        """([a-z]+_[a-z]+)([01])""".toRegex(RegexOption.IGNORE_CASE)


    @ApiOperation(
        value = "Retrieve ticker snapshot",
        notes = "Use this resource to retrieve ticker snapshot"
    )
    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/tickers"])
    suspend fun tickersSnapshot(): Map<Market, Ticker> {
        return poloniexApi.marketTickerStream.first()
    }

    @ApiOperation(
        value = "Retrieve balance snapshot",
        notes = "Use this resource to retrieve balance snapshot"
    )
    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/balances"])
    suspend fun balancesSnapshot(): Map<Currency, Tuple2<Amount, Amount>> {
        return poloniexApi.balanceStream.first()
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/paths"])
    suspend fun pathsSnapshot(@RequestParam initAmount: Amount, @RequestParam currencies: List<Currency>): List<ExhaustivePath> {
        return indicators.getPathsPolling(PathsSettings(initAmount, currencies.toVavrList()))
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

    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/paths2"])
    suspend fun pathsSnapshot2(
        @RequestParam(defaultValue = "USDT") fromCurrency: Currency,
        @RequestParam(defaultValue = "40") fromAmount: Amount,
        @RequestParam(defaultValue = "USDT, USDC") endCurrencies: List<Currency>
    ): TreeSet<ExhaustivePath> {
        return indicators.getPaths(
            fromCurrency,
            fromAmount,
            endCurrencies.toVavrList(),
            fun(p): Boolean {
                val targetMarket = p.chain.lastOrNull() ?: return false
                return fromAmount < targetMarket.toAmount
            },
            Comparator { p0, p1 ->
                if (p0.id == p1.id) {
                    0
                } else {
                    p0.profitability.compareTo(p1.profitability)
                }
            }).take(100)
    }


    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/unfilled/{id}/execute"])
    suspend fun executeFullTransaction(@PathVariable id: Long) {
        poloniexTrader.startPathTranFromUnfilledTrans(id)
    }

    @RequestMapping(
        method = [RequestMethod.POST],
        value = ["/transactions/dsl/execute"],
        consumes = [MediaType.TEXT_PLAIN_VALUE]
    )
    @ApiImplicitParams(
        ApiImplicitParam(
            name = "dslFormattedBody",
            example = "USDT USDC 40.00 USDT_BTC1BTC_USDC0"
        )
    )
    suspend fun executeTransaction(@RequestBody dslFormattedBody: String) {
        val res = execTranBodyPattern.matchEntire(dslFormattedBody) ?: throw Exception("Not correct input")
        val (fromCurrency, targetCurrency, fromCurrencyAmountStr, pathDsl) = res.destructured
        val fromCurrencyAmount = BigDecimal(fromCurrencyAmountStr)

        val markets = execTranPathPattern.findAll(pathDsl).map {
            val (marketStr, speedTypeStr) = it.destructured
            val market = marketStr.toMarket()
            val speedType = if (speedTypeStr == "0") {
                OrderSpeed.Instant
            } else {
                OrderSpeed.Delayed
            }
            tuple(market, speedType)
        }.toVavrStream()

        var i = 0
        var fromCurrencyIt = fromCurrency

        val chain = markets.map {
            val (market, speed) = it
            val targetCurrencyIt = market.other(fromCurrencyIt)!!

            val order = if (speed == OrderSpeed.Instant) {
                InstantOrder(
                    market,
                    fromCurrencyIt,
                    targetCurrencyIt,
                    if (i == 0) fromCurrencyAmount else BigDecimal.ONE,
                    BigDecimal.ONE,
                    market.orderType(targetCurrencyIt)!!,
                    BigDecimal.ONE,
                    BigDecimal.ONE,
                    BigDecimal.ONE,
                    BigDecimal.ONE,
                    list()
                )
            } else {
                DelayedOrder(
                    market,
                    fromCurrencyIt,
                    targetCurrencyIt,
                    if (i == 0) fromCurrencyAmount else BigDecimal.ONE,
                    BigDecimal.ONE,
                    BigDecimal.ONE,
                    BigDecimal.ONE,
                    BigDecimal.ONE,
                    market.orderType(targetCurrencyIt)!!,
                    BigDecimal.ONE,
                    TradeStatOrder(tuple(BigDecimal.ONE, BigDecimal.ONE))
                )
            }

            i++
            fromCurrencyIt = targetCurrencyIt

            order
        }

        val path = ExhaustivePath(tuple(fromCurrency, targetCurrency), chain.toVavrList())

        withTimeout(30000) {
            poloniexTrader.tranRequests.send(path)
        }
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/active"])
    suspend fun getActiveTransactions(): List<Tuple2<UUID, Array<TranIntentMarket>>> {
        return transactionsDao.getActive()
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/completed"])
    suspend fun getCompletedTransactions(): List<Tuple4<Long, Array<TranIntentMarket>, Instant, Instant>> {
        return transactionsDao.getCompleted()
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/completed-short"])
    suspend fun getCompletedShortTransactions(): List<Tuple3<Amount, Amount, Long>> {
        return transactionsDao.getCompleted().map { (_, markets, created, completed) ->
            val fromAmount = tranIntentMarketExtensions.fromAmount(markets[0] as TranIntentMarketCompleted)
            val targetAmount = tranIntentMarketExtensions.targetAmount(markets[markets.length() - 1] as TranIntentMarketCompleted)
            val time = completed.epochSecond - created.epochSecond
            tuple(fromAmount, targetAmount, time)
        }
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/balances-in-use"])
    suspend fun getBalancesInUse(@RequestParam primaryCurrencies: List<Currency>): List<Tuple2<Currency, BigDecimal>> {
        return transactionsDao.balancesInUse(primaryCurrencies.toVavrList())
    }


    //@MessageMapping("/tickers")
    @RequestMapping(method = [RequestMethod.GET], value = ["/tickers"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun tickers() = run {
        poloniexApi.marketTickerStream.sample(Duration.ofSeconds(1).toMillis())
    }

    //@MessageMapping("/paths")
    @RequestMapping(method = [RequestMethod.GET], value = ["/paths"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun paths(
        @RequestParam(required = true) initAmount: Amount,
        @RequestParam(required = true) currencies: List<Currency>
    ) = run {
        indicators.getPathsPolling(PathsSettings(initAmount, currencies.toVavrList()))
            .sampleFirst(Duration.ofSeconds(30))
            .onBackpressureLatest()
            .flatMapSequential({
                Flux.fromIterable(it)
                    .buffer(250)
                    .subscribeOn(Schedulers.elastic())
            }, 1, 1)
    }
}
