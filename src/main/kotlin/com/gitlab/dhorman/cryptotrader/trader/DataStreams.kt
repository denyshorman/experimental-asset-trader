package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.buyBaseAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import io.vavr.Tuple2
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import io.vavr.collection.Set
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.getOrNull
import io.vavr.kotlin.tuple
import kotlinx.coroutines.*
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.collect
import kotlinx.coroutines.reactor.flux
import mu.KotlinLogging
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.*

typealias MarketIntMap = Map<MarketId, Market>
typealias MarketStringMap = Map<Market, MarketId>
typealias MarketData = Tuple2<MarketIntMap, MarketStringMap>

data class OrderBookData(
    val market: Market,
    val marketId: MarketId,
    val book: PriceAggregatedBook,
    val notification: OrderBookNotification
)
typealias OrderBookDataMap = Map<MarketId, Flux<OrderBookData>>

data class BookOrder(
    val market: Market,
    val price: Price,
    val orderType: OrderType
)

private object BalancesAndCurrenciesNotInSync : Exception("", null, true, false)

@Component
class DataStreams(private val poloniexApi: PoloniexApi) {
    private val logger = KotlinLogging.logger {}

    val currencies: Flux<Tuple2<Map<Currency, CurrencyDetails>, Map<Int, Currency>>> = run {
        FlowScope.flux {
            while (true) {
                try {
                    val currencies = poloniexApi.currencies().awaitSingle()
                    send(tuple(currencies, currencies.map { k, v -> tuple(v.id, k) }))
                    delay(10 * 60 * 1000)
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch currencies from Poloniex because ${e.message}")
                    delay(2000)
                }
            }
        }.cache(1)
    }

    val balances: Flux<Map<Currency, Tuple2<Amount, Amount>>> = run {
        FlowScope.flux {
            mainLoop@ while (true) {
                try {
                    val rawApiBalances = poloniexApi.completeBalances()

                    var allOpenOrders = poloniexApi.allOpenOrders().awaitSingle()

                    // Check if open order balance equal to complete onOrder balance
                    val balanceOnOrders = allOpenOrders.groupBy({ (_, order) ->
                        if (order.type == OrderType.Buy) {
                            order.market.baseCurrency
                        } else {
                            order.market.quoteCurrency
                        }
                    }, { (_, order) ->
                        if (order.type == OrderType.Buy) {
                            buyBaseAmount(order.amount, order.price)
                        } else {
                            order.amount
                        }
                    }).mapValues { it.value.reduce { a, b -> a + b } }

                    for ((currency, balance) in rawApiBalances.iterator().map { (c, b) -> tuple(c, b.onOrders) }) {
                        val orderBalance = balanceOnOrders.getOrDefault(currency, BigDecimal.ZERO)

                        if (orderBalance.compareTo(balance) != 0) {
                            logger.warn("Balances ($balance, $orderBalance) not equal for currency $currency")
                            continue@mainLoop
                        }
                    }

                    var availableAndOnOrderBalances =
                        rawApiBalances.mapValues { it.available }.map { currency, availableBalance ->
                            tuple(
                                currency,
                                tuple(availableBalance, balanceOnOrders.getOrDefault(currency, BigDecimal.ZERO))
                            )
                        }

                    send(availableAndOnOrderBalances)

                    val currenciesSnapshot = currencies.awaitFirst()

                    var balanceUpdateDeltaJob: Job? = null

                    fun CoroutineScope.balanceDeltaUpdateJob() = this.launch {
                        poloniexApi.accountNotificationStream.collect { deltaUpdates ->
                            var notifySubscribers = false

                            for (delta in deltaUpdates) {
                                if (delta is BalanceUpdate) {
                                    if (delta.walletType == WalletType.Exchange) {
                                        val currencyId = delta.currencyId
                                        val currency = currenciesSnapshot._2.getOrNull(currencyId)
                                        val availableOnOrdersBalance =
                                            currency?.run { availableAndOnOrderBalances.getOrNull(this) }

                                        if (currency == null || availableOnOrdersBalance == null) {
                                            logger.warn("Balances and currencies are not in sync.")
                                            throw BalancesAndCurrenciesNotInSync
                                        }

                                        val (available, onOrders) = availableOnOrdersBalance
                                        val newBalance = available + delta.amount
                                        availableAndOnOrderBalances =
                                            availableAndOnOrderBalances.put(currency, tuple(newBalance, onOrders))

                                        notifySubscribers = true
                                    }
                                } else if (delta is LimitOrderCreated) {
                                    val marketId = delta.marketId
                                    val market = markets.awaitFirst()._1.getOrNull(marketId)

                                    if (market == null) {
                                        logger.warn("Balances and currencies are not in sync.")
                                        throw BalancesAndCurrenciesNotInSync
                                    }

                                    // 1. Add created order to orders list

                                    allOpenOrders = allOpenOrders.put(
                                        delta.orderId, OpenOrderWithMarket(
                                            delta.orderId,
                                            delta.orderType,
                                            delta.price,
                                            delta.amount,
                                            delta.amount,
                                            delta.amount,
                                            LocalDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")),
                                            false,
                                            market
                                        )
                                    )

                                    val currency: Currency
                                    val deltaOnOrdersAmount: Amount

                                    if (delta.orderType == OrderType.Buy) {
                                        currency = market.baseCurrency
                                        deltaOnOrdersAmount = buyBaseAmount(delta.amount, delta.price)
                                    } else {
                                        currency = market.quoteCurrency
                                        deltaOnOrdersAmount = delta.amount
                                    }

                                    val availableOnOrdersBalance = availableAndOnOrderBalances.getOrNull(currency)

                                    if (availableOnOrdersBalance == null) {
                                        logger.warn("Can't find balance by currency $currency")
                                        throw BalancesAndCurrenciesNotInSync
                                    }

                                    val (available, onOrders) = availableOnOrdersBalance
                                    val newOnOrders = onOrders + deltaOnOrdersAmount
                                    availableAndOnOrderBalances =
                                        availableAndOnOrderBalances.put(currency, tuple(available, newOnOrders))

                                    notifySubscribers = true
                                } else if (delta is OrderUpdate) {
                                    val oldOrder = allOpenOrders.getOrNull(delta.orderId)

                                    if (oldOrder == null) {
                                        val msg = "Order ${delta.orderId} not found in local cache"
                                        logger.warn(msg)
                                        throw Exception(msg)
                                    }

                                    val oldOrderAmount: BigDecimal
                                    val newOrderAmount: BigDecimal
                                    val balanceCurrency: Currency

                                    if (oldOrder.type == OrderType.Buy) {
                                        oldOrderAmount = buyBaseAmount(oldOrder.amount, oldOrder.price)
                                        newOrderAmount = buyBaseAmount(delta.newAmount, oldOrder.price)
                                        balanceCurrency = oldOrder.market.baseCurrency
                                    } else {
                                        oldOrderAmount = oldOrder.amount
                                        newOrderAmount = delta.newAmount
                                        balanceCurrency = oldOrder.market.quoteCurrency
                                    }

                                    // 1. Adjust open orders map

                                    if (delta.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                        allOpenOrders = allOpenOrders.remove(delta.orderId)
                                    } else {
                                        val newOrder = OpenOrderWithMarket(
                                            oldOrder.orderId,
                                            oldOrder.type,
                                            oldOrder.price,
                                            oldOrder.startingAmount,
                                            delta.newAmount,
                                            newOrderAmount,
                                            oldOrder.date,
                                            oldOrder.margin,
                                            oldOrder.market
                                        )

                                        allOpenOrders = allOpenOrders.put(oldOrder.orderId, newOrder)
                                    }

                                    // 2. Adjust on orders balance

                                    val availableOnOrdersBalance =
                                        availableAndOnOrderBalances.getOrNull(balanceCurrency)

                                    if (availableOnOrdersBalance == null) {
                                        logger.warn("Balances and currencies are not in sync.")
                                        throw BalancesAndCurrenciesNotInSync
                                    }

                                    val (available, onOrders) = availableOnOrdersBalance
                                    val newOnOrders = onOrders - oldOrderAmount + newOrderAmount
                                    availableAndOnOrderBalances =
                                        availableAndOnOrderBalances.put(balanceCurrency, tuple(available, newOnOrders))

                                    notifySubscribers = true
                                }
                            }

                            if (notifySubscribers) this@flux.send(availableAndOnOrderBalances)
                        }
                    }

                    coroutineScope {
                        poloniexApi.connection.collect { connected ->
                            if (balanceUpdateDeltaJob != null) {
                                balanceUpdateDeltaJob!!.cancelAndJoin()
                                balanceUpdateDeltaJob = null
                            }

                            if (connected) {
                                balanceUpdateDeltaJob = balanceDeltaUpdateJob()
                            }
                        }
                    }
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                    continue
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn(e.message)
                    delay(1000)
                    continue
                }
            }
        }.cache(1)
    }

    val markets: Flux<MarketData> = run {
        FlowScope.flux {
            var prevMarketsSet = mutableSetOf<Int>()

            while (true) {
                try {
                    val tickers = poloniexApi.ticker().awaitSingle()
                    var marketIntStringMap: Map<MarketId, Market> = HashMap.empty()
                    var marketStringIntMap: Map<Market, MarketId> = HashMap.empty()
                    val currentMarketsSet = mutableSetOf<Int>()

                    for ((market, tick) in tickers) {
                        if (!tick.isFrozen) {
                            marketIntStringMap = marketIntStringMap.put(tick.id, market)
                            marketStringIntMap = marketStringIntMap.put(market, tick.id)
                            currentMarketsSet.add(tick.id)
                        }
                    }

                    if (!prevMarketsSet.containsAll(currentMarketsSet)) {
                        prevMarketsSet = currentMarketsSet
                        send(tuple(marketIntStringMap, marketStringIntMap))
                    }

                    delay(10 * 60 * 1000)
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch markets from Poloniex: ${e.message}")
                    delay(2000)
                }
            }
        }.cache(1)
    }

    val tradesStat: Flux<Map<MarketId, Flux<TradeStat>>> = run {
        FlowScope.flux {
            dayVolume.collect { dayVolumeMap ->
                val marketsMap = markets.awaitFirst()._2
                val map = marketsMap.map { market, marketId ->
                    val amount = dayVolumeMap.getOrNull(market)
                    val amountBase = amount?._1?.divide(BigDecimal(2), 8, RoundingMode.DOWN)
                    val amountQuote = amount?._2?.divide(BigDecimal(2), 8, RoundingMode.DOWN)
                    val buySellStat = TradeStatOrder(
                        baseQuoteAvgAmount = tuple(
                            amountBase ?: BigDecimal.ZERO,
                            amountQuote ?: BigDecimal.ZERO
                        )
                    )
                    val tradeStat = TradeStat(
                        sell = buySellStat,
                        buy = buySellStat
                    )
                    tuple(marketId, Flux.just(tradeStat))
                }

                send(map)
            }

        }.replay(1).refCount(1, Duration.ofMinutes(20))
    }

    val openOrders: Flux<Map<Long, OpenOrderWithMarket>> = run {
        FlowScope.flux {
            while (true) {
                try {
                    var allOpenOrders = poloniexApi.allOpenOrders().awaitSingle()
                    send(allOpenOrders)

                    coroutineScope {
                        launch {
                            poloniexApi.connection.collect { connected ->
                                if (!connected) throw Exception("Connection is closed")
                            }
                        }

                        poloniexApi.accountNotificationStream.collect { notifications ->
                            for (update in notifications) {
                                when (update) {
                                    is LimitOrderCreated -> run {
                                        val marketId = markets.awaitFirst()._1.getOrNull(update.marketId)

                                        if (marketId != null) {
                                            val newOrder = OpenOrderWithMarket(
                                                update.orderId,
                                                update.orderType,
                                                update.price,
                                                update.amount,
                                                update.amount,
                                                update.price * update.amount, // TODO: Incorrect arguments supplied
                                                update.date,
                                                false,
                                                marketId
                                            )

                                            allOpenOrders = allOpenOrders.put(newOrder.orderId, newOrder)
                                            send(allOpenOrders)
                                        } else {
                                            throw Exception("Market id not found in local cache")
                                        }
                                    }
                                    is OrderUpdate -> run {
                                        if (update.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                            allOpenOrders = allOpenOrders.remove(update.orderId)
                                            send(allOpenOrders)
                                        } else {
                                            val order = allOpenOrders.getOrNull(update.orderId)

                                            if (order != null) {
                                                val newOrder = OpenOrderWithMarket(
                                                    order.orderId,
                                                    order.type,
                                                    order.price,
                                                    order.startingAmount,
                                                    update.newAmount,
                                                    order.total, // TODO: Incorrect value supplied
                                                    order.date,
                                                    order.margin,
                                                    order.market
                                                )

                                                allOpenOrders = allOpenOrders.put(order.orderId, newOrder)
                                                send(allOpenOrders)
                                            } else {
                                                throw Exception("Order not found in local cache")
                                            }
                                        }
                                    }
                                    else -> run { /*ignore*/ }
                                }
                            }
                        }
                    }
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                    continue
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't update open order: ${e.message}")
                    delay(1000)
                    continue
                }
            }
        }.cache(1)
    }

    // TODO: Optimize calculation
    val orderBookOrders: Flux<Set<BookOrder>> = run {
        FlowScope.flux {
            openOrders.collect { openOrdersMap ->
                val orderBookOrdersSet = openOrdersMap.map { openOrder ->
                    BookOrder(
                        openOrder._2.market,
                        openOrder._2.price,
                        openOrder._2.type
                    )
                }.toSet()

                send(orderBookOrdersSet)
            }
        }.cache(1)
    }

    val tickers: Flux<Map<Market, Ticker>> = run {
        fun mapTicker(m: Market, t: Ticker0): Tuple2<Market, Ticker> {
            return tuple(
                m,
                Ticker(
                    t.id,
                    t.last,
                    t.lowestAsk,
                    t.highestBid,
                    t.percentChange,
                    t.baseVolume,
                    t.quoteVolume,
                    t.isFrozen,
                    t.high24hr,
                    t.low24hr
                )
            )
        }

        FlowScope.flux {
            while (true) {
                try {
                    var allTickers = poloniexApi.ticker().awaitSingle().map(::mapTicker)
                    send(allTickers)

                    coroutineScope {
                        launch {
                            poloniexApi.connection.collect { connected ->
                                if (!connected) throw Exception("Can't trust tickers because connection is closed")
                            }
                        }

                        poloniexApi.tickerStream.onBackpressureLatest().collect { ticker ->
                            val marketId = markets.awaitFirst()._1.getOrNull(ticker.id)

                            if (marketId != null) {
                                allTickers = allTickers.put(marketId, ticker)
                                send(allTickers)
                            } else {
                                throw Exception("Market for ticker not found in local cache.")
                            }
                        }
                    }
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                    continue
                } catch (e: Exception) {
                    delay(1000)
                    continue
                }
            }
        }.cache(1)
    }

    val orderBooks: Flux<OrderBookDataMap> = run {
        markets.map { marketInfo ->
            val marketIds = marketInfo._1.keySet()

            poloniexApi.orderBooksStream(marketIds).map { marketId, bookStream ->

                val newBookStream = bookStream.map { (book, update) ->
                    OrderBookData(marketInfo._1.get(marketId).get(), marketId, book, update)
                }
                    .replay(1)
                    .refCount(1, Duration.ofMinutes(2))

                Tuple2(marketId, newBookStream)
            }
        }.cache(1)
    }

    val fee: Flux<FeeMultiplier> = run {
        suspend fun fetchFee(): FeeMultiplier {
            val fee = poloniexApi.feeInfo().awaitSingle()
            return FeeMultiplier(fee.makerFee.oneMinus, fee.takerFee.oneMinus)
        }

        FlowScope.flux {
            while (true) {
                try {
                    send(fetchFee())
                    break
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch fee from Poloniex because ${e.message}")
                    delay(2000)
                }
            }

            // TODO: How to get fee instantly without pooling ?
            while (true) {
                delay(10 * 60 * 1000)

                try {
                    send(fetchFee())
                    break
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch fee from Poloniex because ${e.message}")
                }
            }
        }.cache(1)
    }

    val dayVolume: Flux<Map<Market, Tuple2<Amount, Amount>>> = run {
        FlowScope.flux {
            while (true) {
                try {
                    send(poloniexApi.dayVolume())
                    delay(3 * 60 * 1000)
                } catch (e: CancellationException) {
                    if (!isActive) throw e
                    delay(1000)
                } catch (e: Exception) {
                    logger.debug { "Can't fetch day volume from Poloniex because ${e.message}" }
                    delay(2000)
                }
            }
        }.replay(1).refCount(1, Duration.ofMinutes(20))
    }

    suspend fun getMarketId(market: Market): MarketId? {
        return markets.awaitFirst()._2.getOrNull(market)
    }

    suspend fun getMarket(marketId: MarketId): Market? {
        return markets.awaitFirst()._1.getOrNull(marketId)
    }

    suspend fun getOrderBookFlowBy(market: Market): Flux<OrderBookAbstract> {
        val marketId = getMarketId(market) ?: throw Exception("Market not found")
        return (orderBooks.awaitFirst().getOrNull(marketId)
            ?: throw Exception("Order book for $marketId not found")).map { it.book as OrderBookAbstract }
    }
}