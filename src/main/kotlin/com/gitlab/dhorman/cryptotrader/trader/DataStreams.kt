package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.trader.data.tradestat.*
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import io.vavr.Tuple2
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import io.vavr.collection.Queue
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
import java.time.Duration
import java.time.ZoneOffset

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
            while (isActive) {
                try {
                    val currencies = poloniexApi.currencies().awaitSingle()
                    send(tuple(currencies, currencies.map { k, v -> tuple(v.id, k) }))
                    delay(10 * 60 * 1000)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch currencies from Poloniex because ${e.message}")
                    delay(2000)
                }
            }
        }.cache(1)
    }

    // TODO: Review balance updates!
    val balances: Flux<Map<Currency, Amount>> = run {
        FlowScope.flux {
            while (isActive) {
                try {
                    var allBalances = poloniexApi.completeBalances().mapValues { it.available + it.onOrders }
                    send(allBalances)

                    val currenciesSnapshot = currencies.awaitFirst()

                    var balanceUpdateDeltaJob: Job? = null

                    fun CoroutineScope.balanceDeltaUpdateJob() = this.launch {
                        poloniexApi.accountNotificationStream.onBackpressureBuffer().collect { balanceDelta ->
                            if (balanceDelta !is BalanceUpdate) return@collect

                            if (balanceDelta.walletType == WalletType.Exchange) {
                                val currencyId = balanceDelta.currencyId
                                val currency = currenciesSnapshot._2.getOrNull(currencyId)
                                val balance = currency?.run { allBalances.getOrNull(this) }

                                if (currency != null && balance != null) {
                                    val newBalance = balance + balanceDelta.amount

                                    allBalances = allBalances.put(currency, newBalance)
                                    this@flux.send(allBalances)
                                } else {
                                    logger.warn("Balances and currencies are not in sync.")
                                    throw BalancesAndCurrenciesNotInSync
                                }
                            }
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
                    throw e
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn(e.message, e)
                    delay(1000)
                    continue
                }
            }
        }.cache(1)
    }

    val markets: Flux<MarketData> = run {
        FlowScope.flux {
            var prevMarketsSet = mutableSetOf<Int>()

            while (isActive) {
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
                    throw e
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch markets from Poloniex: ${e.message}")
                    delay(2000)
                }
            }
        }.cache(1)
    }

    val tradesStat: Flux<Map<MarketId, Flux<TradeStat>>> = run {
        val bufferLimit = 100

        markets.map { (_, marketStringMap) ->
            marketStringMap.map { market, marketId ->
                val tradesFlow = FlowScope.flux {
                    while (isActive) {
                        try {
                            var trade1 = poloniexApi.tradeHistoryPublic(market).awaitSingle().run {
                                var sellTrades = Queue.empty<SimpleTrade>()
                                var buyTrades = Queue.empty<SimpleTrade>()

                                for (trade in this) {
                                    val trade0 =
                                        SimpleTrade(trade.price, trade.amount, trade.date.toInstant(ZoneOffset.UTC))

                                    if (trade.type == OrderType.Sell) {
                                        sellTrades = sellTrades.append(trade0)
                                    } else {
                                        buyTrades = buyTrades.append(trade0)
                                    }
                                }

                                if (sellTrades.length() > bufferLimit)
                                    sellTrades = sellTrades.dropRight(sellTrades.length() - bufferLimit)

                                if (buyTrades.length() > bufferLimit)
                                    buyTrades = buyTrades.dropRight(buyTrades.length() - bufferLimit)

                                Trade1(
                                    sellOld = sellTrades,
                                    sellNew = sellTrades,
                                    buyOld = buyTrades,
                                    buyNew = buyTrades,
                                    sellStatus = Trade1Status.Init,
                                    buyStatus = Trade1Status.Init
                                )
                            }
                            var trade2 = Trade2.DEFAULT

                            val orderBookTradesFlow = orderBooks.awaitFirst().getOrNull(marketId)
                                ?.handle<OrderBookTrade> { orderBookData, sink ->
                                    val trade = orderBookData.notification as? OrderBookTrade
                                    if (trade != null) sink.next(trade)
                                }
                                ?: throw Exception("Can't find order book by specified market")

                            fun calcTrade1(bookTrade: OrderBookTrade) {
                                val newTrade = SimpleTrade(bookTrade.price, bookTrade.amount, bookTrade.timestamp)
                                val sellOld: Queue<SimpleTrade>
                                val sellNew: Queue<SimpleTrade>
                                val buyOld: Queue<SimpleTrade>
                                val buyNew: Queue<SimpleTrade>
                                val sellStatus: Trade1Status
                                val buyStatus: Trade1Status

                                if (bookTrade.orderType == OrderType.Sell) {
                                    sellOld = trade1.sellNew
                                    sellNew = Trade1.newTrades(newTrade, trade1.sellNew, bufferLimit)
                                    buyOld = trade1.buyOld
                                    buyNew = trade1.buyNew
                                    sellStatus = Trade1Status.Changed
                                    buyStatus = Trade1Status.NotChanged
                                } else {
                                    buyOld = trade1.buyNew
                                    buyNew = Trade1.newTrades(newTrade, trade1.buyNew, bufferLimit)
                                    sellOld = trade1.sellOld
                                    sellNew = trade1.sellNew
                                    buyStatus = Trade1Status.Changed
                                    sellStatus = Trade1Status.NotChanged
                                }

                                trade1 = Trade1(sellOld, sellNew, buyOld, buyNew, sellStatus, buyStatus)
                            }

                            fun calcTrade2() {
                                val sell = when (trade1.sellStatus) {
                                    Trade1Status.Changed -> Trade2State.calc(
                                        trade2.sell,
                                        trade1.sellOld,
                                        trade1.sellNew
                                    )
                                    Trade1Status.NotChanged -> trade2.sell
                                    Trade1Status.Init -> Trade2State.calcFull(trade1.sellNew)
                                }

                                val buy = when (trade1.buyStatus) {
                                    Trade1Status.Changed -> Trade2State.calc(
                                        trade2.buy,
                                        trade1.buyOld,
                                        trade1.buyNew
                                    )
                                    Trade1Status.NotChanged -> trade2.buy
                                    Trade1Status.Init -> Trade2State.calcFull(trade1.buyNew)
                                }

                                trade2 = Trade2(sell, buy)
                            }

                            suspend fun sendStat() {
                                send(TradeStat(Trade2State.map(trade2.sell), Trade2State.map(trade2.buy)))
                            }

                            calcTrade2()
                            sendStat()

                            coroutineScope {
                                launch {
                                    poloniexApi.connection.collect { connected ->
                                        if (!connected) throw Exception("Connection lost")
                                    }
                                }

                                orderBookTradesFlow.onBackpressureBuffer().collect { bookTrade ->
                                    calcTrade1(bookTrade)
                                    calcTrade2()
                                    sendStat()
                                }
                            }
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            if (logger.isDebugEnabled) logger.warn(e.message)
                            delay(1000)
                        }
                    }
                }
                    .replay(1)
                    .refCount(1, Duration.ofMinutes(2))

                tuple(marketId, tradesFlow)
            }
        }.cache(1)
    }

    val openOrders: Flux<Map<Long, OpenOrderWithMarket>> = run {
        FlowScope.flux {
            while (isActive) {
                try {
                    var allOpenOrders = poloniexApi.allOpenOrders().awaitSingle()
                    send(allOpenOrders)

                    coroutineScope {
                        launch {
                            poloniexApi.connection.collect { connected ->
                                if (!connected) throw Exception("Connection is closed")
                            }
                        }

                        poloniexApi.accountNotificationStream.onBackpressureBuffer().collect { update ->
                            when (update) {
                                is LimitOrderCreated -> run {
                                    val marketId = markets.awaitFirst()._1.getOrNull(update.marketId)

                                    if (marketId != null) {
                                        val newOrder = OpenOrderWithMarket(
                                            update.orderNumber,
                                            update.orderType,
                                            update.rate,
                                            update.amount,
                                            update.amount,
                                            update.rate * update.amount, // TODO: Incorrect arguments supplied
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
                                                order.date, // TODO: Incorrect value supplied
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
                } catch (e: CancellationException) {
                    throw e
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
            while (isActive) {
                try {
                    var allTickers = poloniexApi.ticker().awaitSingle().map(::mapTicker)
                    send(allTickers)

                    coroutineScope {
                        launch {
                            poloniexApi.connection.collect { connected ->
                                if (!connected) throw Exception("Can't trust tickers because connection is closed")
                            }
                        }

                        poloniexApi.tickerStream.onBackpressureDrop().collect { ticker ->
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
                    throw e
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
            while (isActive) {
                try {
                    send(fetchFee())
                    break
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch fee from Poloniex because ${e.message}")
                    delay(2000)
                }
            }

            // TODO: How to get fee instantly without pooling ?
            while (isActive) {
                delay(10 * 60 * 1000)

                try {
                    send(fetchFee())
                    break
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    if (logger.isDebugEnabled) logger.warn("Can't fetch fee from Poloniex because ${e.message}")
                }
            }
        }.cache(1)
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