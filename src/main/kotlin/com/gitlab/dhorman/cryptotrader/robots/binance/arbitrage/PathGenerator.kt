package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage

import com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model.*
import com.gitlab.dhorman.cryptotrader.service.binance.BinanceApi
import com.gitlab.dhorman.cryptotrader.util.share
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.hours
import kotlin.time.toJavaDuration

@Component("BinancePathGenerator")
class PathGenerator(
    private val binanceApi: BinanceApi
) {
    private val logger = KotlinLogging.logger {}
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    init {
        Runtime.getRuntime().addShutdownHook(Thread { runBlocking { close() } })
    }

    suspend fun close() {
        scope.coroutineContext[Job]?.cancelAndJoin()
    }

    //region Streams
    val symbol24HourStat: Flow<Map<String, StateFlow<BinanceApi.TickerEvent>>> = run {
        flow {
            val symbolLastTicker = ConcurrentHashMap<String, MutableStateFlow<BinanceApi.TickerEvent>>()
            binanceApi.allMarketTickersStream.collect { event ->
                if (event.payload == null || event.payload.isEmpty()) return@collect
                event.payload.forEach { ticker ->
                    val state = symbolLastTicker[ticker.symbol]
                    if (state == null) {
                        symbolLastTicker[ticker.symbol] = MutableStateFlow(ticker)
                    } else {
                        state.value = ticker
                    }
                }
                emit(symbolLastTicker)
            }
        }.share(1, 6.hours.toJavaDuration(), scope)
    }

    val orderBookLastTick: Flow<Map<String, StateFlow<BinanceApi.BookTickerEvent>>> = run {
        flow {
            val symbolBookLastTicker = ConcurrentHashMap<String, MutableStateFlow<BinanceApi.BookTickerEvent>>()
            binanceApi.allBookTickerStream.collect { event ->
                if (event.payload == null) return@collect
                val state = symbolBookLastTicker[event.payload.symbol]
                if (state == null) {
                    symbolBookLastTicker[event.payload.symbol] = MutableStateFlow(event.payload)
                } else {
                    state.value = event.payload
                }
                emit(symbolBookLastTicker)
            }
        }.share(1, 6.hours.toJavaDuration(), scope)
    }
    //endregion

    suspend fun generate(fromCurrency: Currency, toCurrencies: Iterable<Currency>): Sequence<SimulatedPath> {
        val exchangeInfo = binanceApi.exchangeInfoCache.first()
        val symbols = exchangeInfo.symbols

        logger.debug("Generating path ($fromCurrency -> $toCurrencies)...")

        coroutineScope {
            val desiredSize = symbols.size / 2

            logger.debug { "Waiting for the tickers to fill" }

            launch {
                orderBookLastTick.takeWhile { it.size < desiredSize }.collect()
            }

            launch {
                symbol24HourStat.takeWhile { it.size < desiredSize }.collect()
            }
        }

        logger.debug("Start path generation...")

        val symbol24HourStatMap = symbol24HourStat.first()
        val orderBookLastTickMap = orderBookLastTick.first()

        val markets = symbols
            .asSequence()
            .filter { symbol24HourStatMap.containsKey(it.symbol) && orderBookLastTickMap.containsKey(it.symbol) }
            .map { it.toMarket() }
            .asIterable()

        return MarketPathGenerator(markets)
            .generateWithOrders(listOf(fromCurrency), toCurrencies)
            .asSequence()
            .flatMap { (_, paths) ->
                paths.map { path ->
                    val orderIntents = path.map { (speed, market) -> SimulatedPath.OrderIntent(market, speed) }
                    SimulatedPath(orderIntents.toList())
                }
            }
    }

    //region Private Extensions
    private fun BinanceApi.ExchangeInfo.Symbol.toMarket() = Market(baseAsset, quoteAsset)
    //endregion
}

//region Costants
private val INT_MAX_VALUE_BIG_DECIMAL = Int.MAX_VALUE.toBigDecimal()
private val ALMOST_ZERO = BigDecimal("0.00000001")
//endregion

//region Metrics
fun SimulatedPath.OrderIntent.targetAmount(
    fromCurrency: Currency,
    fromCurrencyAmount: Amount,
    fee: BinanceApi.CachedFee,
    orderBook: BinanceApi.BookTickerEvent,
    symbol: BinanceApi.ExchangeInfo.Symbol,
    amountCalculator: BuySellAmountCalculator
): Amount {
    val orderTpe = when (fromCurrency) {
        market.baseCurrency -> OrderType.Buy
        market.quoteCurrency -> OrderType.Sell
        else -> throw RuntimeException("Currency $fromCurrency does not exist in market $market")
    }

    val limit = symbol.filtersIndexed[BinanceApi.ExchangeInfo.Symbol.Filter.Type.LOT_SIZE]?.stepSize
        ?: throw Exception("LOT_SIZE limit is not defined for market $market")

    return when (orderSpeed) {
        OrderSpeed.Instant -> {
            when (orderTpe) {
                OrderType.Buy -> {
                    val tradeQuoteAmount = amountCalculator.quoteAmount(
                        BuySellAmountCalculator.Data(
                            fromCurrencyAmount,
                            orderBook.bestAskPrice,
                            fee.takerMultiplier,
                            symbol.baseAssetPrecision,
                            symbol.quoteAssetPrecision
                        )
                    ).scaleToLimit(limit)

                    amountCalculator.targetAmountBuy(
                        BuySellAmountCalculator.Data(
                            tradeQuoteAmount,
                            orderBook.bestAskPrice,
                            fee.takerMultiplier,
                            symbol.baseAssetPrecision,
                            symbol.quoteAssetPrecision
                        )
                    )
                }
                OrderType.Sell -> {
                    amountCalculator.targetAmountSell(
                        BuySellAmountCalculator.Data(
                            fromCurrencyAmount.scaleToLimit(limit),
                            orderBook.bestBidPrice,
                            fee.takerMultiplier,
                            symbol.baseAssetPrecision,
                            symbol.quoteAssetPrecision
                        )
                    )
                }
            }
        }
        OrderSpeed.Delayed -> {
            when (orderTpe) {
                OrderType.Buy -> {
                    val price = orderBook.bestBidPrice

                    val quoteAmount = amountCalculator.quoteAmount(
                        BuySellAmountCalculator.Data(
                            fromCurrencyAmount,
                            price,
                            fee.makerMultiplier,
                            symbol.baseAssetPrecision,
                            symbol.quoteAssetPrecision
                        )
                    ).scaleToLimit(limit)

                    amountCalculator.targetAmountBuy(
                        BuySellAmountCalculator.Data(
                            quoteAmount,
                            price,
                            fee.makerMultiplier,
                            symbol.baseAssetPrecision,
                            symbol.quoteAssetPrecision
                        )
                    )
                }
                OrderType.Sell -> {
                    amountCalculator.targetAmountSell(
                        BuySellAmountCalculator.Data(
                            fromCurrencyAmount.scaleToLimit(limit),
                            orderBook.bestAskPrice,
                            fee.makerMultiplier,
                            symbol.baseAssetPrecision,
                            symbol.quoteAssetPrecision
                        )
                    )
                }
            }
        }
    }
}

fun SimulatedPath.amounts(
    fromCurrency: Currency,
    fromCurrencyAmount: Amount,
    symbolFee: Map<String, BinanceApi.CachedFee>,
    orderBooks: Map<String, BinanceApi.BookTickerEvent>,
    symbols: Map<String, BinanceApi.ExchangeInfo.Symbol>,
    amountCalculator: BuySellAmountCalculator
): Array<Pair<Amount, Amount>> {
    var currency = fromCurrency
    var amount = fromCurrencyAmount
    val orderIntentIterator = orderIntents.iterator()

    return Array(orderIntents.size) {
        val orderIntent = orderIntentIterator.next()

        val orderBook = orderBooks[orderIntent.market.symbol]
            ?: throw Exception("Order book for market ${orderIntent.market} does not exist in map")

        val fee = symbolFee[orderIntent.market.symbol]
            ?: throw Exception("Fee not found for market ${orderIntent.market}")

        val symbol = symbols[orderIntent.market.symbol]
            ?: throw Exception("Symbol not found for market ${orderIntent.market}")

        val fromAmount = amount

        amount = orderIntent.targetAmount(
            currency,
            fromAmount,
            fee,
            orderBook,
            symbol,
            amountCalculator
        )

        currency = orderIntent.market.other(currency)
            ?: throw RuntimeException("Currency $currency does not exist in market ${orderIntent.market}")

        Pair(fromAmount, amount)
    }
}

fun SimulatedPath.targetAmount(
    fromCurrency: Currency,
    fromCurrencyAmount: Amount,
    symbolFee: Map<String, BinanceApi.CachedFee>,
    orderBooks: Map<String, BinanceApi.BookTickerEvent>,
    symbols: Map<String, BinanceApi.ExchangeInfo.Symbol>,
    amountCalculator: BuySellAmountCalculator
): Amount {
    var currency = fromCurrency
    var amount = fromCurrencyAmount

    for (orderIntent in orderIntents) {
        val orderBook = orderBooks[orderIntent.market.symbol]
            ?: throw Exception("Order book for market ${orderIntent.market} does not exist in map")

        val symbol = symbols[orderIntent.market.symbol]
            ?: throw Exception("Symbol ${orderIntent.market.symbol} not found in the map")

        val fee = symbolFee[orderIntent.market.symbol]
            ?: throw Exception("Fee not found for market ${orderIntent.market}")

        amount = orderIntent.targetAmount(
            currency,
            amount,
            fee,
            orderBook,
            symbol,
            amountCalculator
        )

        currency = orderIntent.market.other(currency)
            ?: throw RuntimeException("Currency $currency does not exist in market ${orderIntent.market}")
    }
    return amount
}

fun SimulatedPath.targetAmount(
    fromCurrency: Currency,
    fromCurrencyAmount: Amount,
    symbolFee: Map<String, BinanceApi.CachedFee>,
    orderBooks: Map<String, StateFlow<BinanceApi.BookTickerEvent>>,
    symbols: Map<String, BinanceApi.ExchangeInfo.Symbol>,
    amountCalculator: BuySellAmountCalculator
): Flow<Amount> {
    return flow {
        val priceChanges = orderIntents.asFlow().flatMapMerge(Int.MAX_VALUE) { orderIntent ->
            orderBooks[orderIntent.market.symbol] ?: throw Exception("Can't find order book")
        }

        priceChanges.conflate().collect {
            var currency = fromCurrency
            var amount = fromCurrencyAmount

            for (orderIntent in orderIntents) {
                val orderBook0 = orderBooks[orderIntent.market.symbol]?.value
                    ?: throw Exception("Order book for market ${orderIntent.market} does not exist in map")

                val symbol = symbols[orderIntent.market.symbol]
                    ?: throw Exception("Symbol ${orderIntent.market.symbol} not found in the map")

                val fee = symbolFee[orderIntent.market.symbol]
                    ?: throw Exception("Fee not found for market ${orderIntent.market}")

                amount = orderIntent.targetAmount(
                    currency,
                    amount,
                    fee,
                    orderBook0,
                    symbol,
                    amountCalculator
                )

                currency = orderIntent.market.other(currency)
                    ?: throw RuntimeException("Currency $currency does not exist in market ${orderIntent.market}")
            }

            emit(amount)
        }
    }
}

fun SimulatedPath.OrderIntent.waitTime(
    fromCurrency: Currency,
    fromAmount: Amount,
    tradeVolumeStat: BinanceApi.TickerEvent
): BigDecimal {
    return when (orderSpeed) {
        OrderSpeed.Instant -> ALMOST_ZERO
        OrderSpeed.Delayed -> {
            val fromCurrencyType = market.tpe(fromCurrency)
                ?: throw RuntimeException("Currency $fromCurrency does not exist in market $market")

            val orderType = market.orderType(AmountType.From, fromCurrency)
                ?: throw RuntimeException("Currency $fromCurrency does not exist in market $market")

            val volumePerPeriod = tradeVolumeStat.volume(fromCurrencyType, orderType)

            if (volumePerPeriod.compareTo(BigDecimal.ZERO) == 0) return INT_MAX_VALUE_BIG_DECIMAL

            fromAmount.divide(volumePerPeriod, 16, RoundingMode.HALF_EVEN)
        }
    }
}

fun SimulatedPath.waitTime(
    fromCurrency: Currency,
    tradeVolumeStatMap: Map<String, BinanceApi.TickerEvent>,
    amounts: Array<Pair<Amount, Amount>>
): BigDecimal {
    var currency = fromCurrency
    var waitTimeSum = ALMOST_ZERO
    val amountsIterator = amounts.iterator()
    for (orderIntent in orderIntents) {
        val tradeVolumeStat = tradeVolumeStatMap[orderIntent.market.symbol]
            ?: throw Exception("Volume stat for market ${orderIntent.market} does not exist in map")
        val fromAmount = amountsIterator.next().first
        val waitTime = orderIntent.waitTime(currency, fromAmount, tradeVolumeStat)
        waitTimeSum += if (waitTime.compareTo(INT_MAX_VALUE_BIG_DECIMAL) == 0 && waitTimeSum >= waitTime) BigDecimal.ZERO else waitTime
        currency = orderIntent.market.other(currency) ?: throw RuntimeException("Currency $currency does not exist in market ${orderIntent.market}")
    }
    return waitTimeSum
}
//endregion

//region Extensions
fun BigDecimal.scaleToLimit(limit: BigDecimal): BigDecimal {
    return setScale(limit.stripTrailingZeros().scale(), RoundingMode.DOWN)
}

private fun BinanceApi.TickerEvent.volume(currencyType: CurrencyType, orderType: OrderType): BigDecimal {
    return when (currencyType) {
        CurrencyType.Base -> when (orderType) {
            OrderType.Buy -> totalTradedBaseAssetVolume // baseBuyVolume
            OrderType.Sell -> totalTradedBaseAssetVolume // baseSellVolume
        }
        CurrencyType.Quote -> when (orderType) {
            OrderType.Buy -> totalTradedQuoteAssetVolume // quoteBuyVolume
            OrderType.Sell -> totalTradedQuoteAssetVolume // quoteSellVolume
        }
    }
}
//endregion
