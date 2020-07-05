package com.gitlab.dhorman.cryptotrader.api

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Ticker
import com.gitlab.dhorman.cryptotrader.trader.*
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.dao.SettingsDao
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.model.*
import com.gitlab.dhorman.cryptotrader.util.CsvGenerator
import com.gitlab.dhorman.cryptotrader.util.first
import io.vavr.Tuple2
import io.vavr.Tuple3
import io.vavr.Tuple4
import io.vavr.collection.Array
import io.vavr.collection.Map
import io.vavr.kotlin.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.*

@RestController
@RequestMapping(value = ["/api/traders/poloniex"], produces = [MediaType.APPLICATION_JSON_VALUE])
class PoloniexTraderApi(
    private val poloniexTrader: PoloniexTrader,
    private val transactionsDao: TransactionsDao,
    private val settingsDao: SettingsDao,
    private val poloniexApi: ExtendedPoloniexApi,
    private val pathGenerator: PathGenerator,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator
) {
    private val logger = KotlinLogging.logger {}
    private val tranIntentMarketExtensions = TranIntentMarketExtensions(amountCalculator, poloniexApi)

    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/tickers"])
    suspend fun tickersSnapshot(): Map<Market, Ticker> {
        return poloniexApi.marketTickerStream.first()
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/balances"])
    suspend fun balancesSnapshot(): Map<Currency, Tuple2<Amount, Amount>> {
        return poloniexApi.balanceStream.first()
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/unfilled/{id}/execute"])
    suspend fun executeFullTransaction(@PathVariable id: Long) {
        poloniexTrader.startPathTranFromUnfilledTrans(id)
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/active"])
    suspend fun getActiveTransactions(): List<Tuple2<UUID, Array<TranIntentMarket>>> {
        return transactionsDao.getActive()
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/active/{id}"])
    suspend fun getPathsForActiveTransaction(@PathVariable id: UUID, serverHttpResponse: ServerHttpResponse): Flow<String> {
        serverHttpResponse.headers.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=paths.csv")
        val fee = poloniexApi.feeStream.first()
        val orderBooks = poloniexApi.orderBooksPollingStream.first()
        val tradeVolumeStat = poloniexApi.tradeVolumeStat.first()
        val primaryCurrencies = settingsDao.getPrimaryCurrencies()
        val activeTransaction = transactionsDao.getActive(id) ?: throw Exception("Transaction $id not found")
        val initAmount = when (val tran = activeTransaction[0]) {
            is TranIntentMarketCompleted -> tranIntentMarketExtensions.fromAmount(tran)
            is TranIntentMarketPartiallyCompleted -> tran.fromAmount
            is TranIntentMarketPredicted -> throw Exception("Transaction $id does not have from amount")
        }
        val idx = tranIntentMarketExtensions.partiallyCompletedMarketIndex(activeTransaction)
            ?: throw Exception("Partially completed market not found")
        val currentMarket = activeTransaction[idx] as TranIntentMarketPartiallyCompleted
        val currentCurrency = currentMarket.fromCurrency
        val currentAmount = currentMarket.fromAmount

        val allPaths = pathGenerator.generate(currentCurrency, currentAmount, primaryCurrencies)

        return allPaths.asFlow()
            .transform { path ->
                try {
                    val pathAmountPrediction = path.amounts(currentCurrency, currentAmount, fee, orderBooks, amountCalculator)
                    val profit = pathAmountPrediction.last()._2 - initAmount
                    val waitTime = path.waitTime(currentCurrency, tradeVolumeStat, pathAmountPrediction)
                    val profitability = calcProfitability(profit, waitTime)

                    val line = CsvGenerator.toCsvNewLine(
                        currentCurrency,
                        path.targetCurrency(currentCurrency)!!,
                        path.marketsTinyString(),
                        profit,
                        profitability
                    )

                    emit(line)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                }
            }
            .flowOn(Dispatchers.IO)
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/transactions/active/{id}/best-path"])
    suspend fun findBestPathForActiveTransaction(@PathVariable id: UUID): PathGenerator.PathWithMetrics? {
        val primaryCurrencies = settingsDao.getPrimaryCurrencies()
        val activeTransaction = transactionsDao.getActive(id) ?: throw Exception("Transaction $id not found")
        val initAmount = when (val tran = activeTransaction[0]) {
            is TranIntentMarketCompleted -> tranIntentMarketExtensions.fromAmount(tran)
            is TranIntentMarketPartiallyCompleted -> tran.fromAmount
            is TranIntentMarketPredicted -> throw Exception("Transaction $id does not have from amount")
        }
        val idx = tranIntentMarketExtensions.partiallyCompletedMarketIndex(activeTransaction)
            ?: throw Exception("Partially completed market not found")
        val currentMarket = activeTransaction[idx] as TranIntentMarketPartiallyCompleted
        val currentCurrency = currentMarket.fromCurrency
        val currentAmount = currentMarket.fromAmount

        return try {
            pathGenerator.findPath(initAmount, currentCurrency, currentAmount, primaryCurrencies, id)
        } catch (e: PathCantBeFoundException) {
            null
        }
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
        return transactionsDao.balancesInUse(primaryCurrencies)
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/tickers"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun tickers() = run {
        poloniexApi.marketTickerStream.sample(Duration.ofSeconds(1).toMillis())
    }

    @RequestMapping(method = [RequestMethod.GET], value = ["/snapshots/paths"], produces = [MediaType.APPLICATION_OCTET_STREAM_VALUE])
    suspend fun generatePaths(
        @RequestParam(defaultValue = "100") fromAmount: Amount,
        @RequestParam(defaultValue = "USDT, USDC, USDJ, PAX, DAI") currencies: List<Currency>,
        serverHttpResponse: ServerHttpResponse
    ): Flow<String> {
        serverHttpResponse.headers.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=paths.csv")
        val fee = poloniexApi.feeStream.first()
        val orderBooks = poloniexApi.orderBooksPollingStream.first()
        val tradeVolumeStat = poloniexApi.tradeVolumeStat.first()
        return currencies.asFlow().flatMapMerge(currencies.size) { fromCurrency ->
            flow {
                val allPaths = pathGenerator.generate(fromCurrency, fromAmount, currencies)

                allPaths.asFlow()
                    .transform { path ->
                        try {
                            val pathAmountPrediction = path.amounts(fromCurrency, fromAmount, fee, orderBooks, amountCalculator)
                            val profit = pathAmountPrediction.last()._2 - fromAmount
                            val waitTime = path.waitTime(fromCurrency, tradeVolumeStat, pathAmountPrediction)
                            val profitability = calcProfitability(profit, waitTime)

                            val line = CsvGenerator.toCsvNewLine(
                                fromCurrency,
                                path.targetCurrency(fromCurrency)!!,
                                path.marketsTinyString(),
                                profit,
                                profitability
                            )

                            emit(line)
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Throwable) {
                        }
                    }
                    .collect { emit(it) }
            }
        }.flowOn(Dispatchers.IO)
    }
}
