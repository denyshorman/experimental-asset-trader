package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.FeeMultiplier
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.core.TradeStat
import com.gitlab.dhorman.cryptotrader.core.oneMinus
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import io.vavr.Tuple2
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import io.vavr.collection.Queue
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.tuple
import mu.KotlinLogging
import reactor.core.publisher.Flux
import java.math.BigDecimal
import java.time.ZoneOffset
import java.util.function.BiFunction

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

class DataStreams(
    private val raw: RawStreams,
    private val trigger: TriggerStreams,
    private val poloniexApi: PoloniexApi
) {
    private val logger = KotlinLogging.logger {}

    val currencies: Flux<Tuple2<Map<Currency, CurrencyDetails>, Map<Int, Currency>>> = run {
        raw.currencies.map { curr ->
            Tuple2(curr, curr.map { k, v -> Tuple2(v.id, k) })
        }.cache(1)
    }

    val balances: Flux<Map<Currency, Amount>> = run {
        raw.balances.switchMap { balanceInfo ->
            currencies.switchMap { curr ->
                raw.accountNotifications
                    .filter { it is BalanceUpdate }
                    .map { it as BalanceUpdate }
                    .scan(
                        balanceInfo
                    ) { allBalances, balanceUpdate ->
                        if (balanceUpdate.walletType == WalletType.Exchange) {
                            val currAmountOption = curr._2
                                .get(balanceUpdate.currencyId)
                                .flatMap { currencyId ->
                                    allBalances.get(currencyId).map { balance -> Tuple2(currencyId, balance) }
                                }

                            if (currAmountOption != null) {
                                val (currencyId, balance) = currAmountOption.get()
                                val newBalance = balance + balanceUpdate.amount

                                allBalances.put(currencyId, newBalance)
                            } else {
                                logger.warn("Balances and currencies can be not in sync. Fetch new balances and currencies.")
                                trigger.currencies.onNext(Unit)
                                trigger.balances.onNext(Unit)

                                allBalances
                            }
                        } else {
                            allBalances
                        }
                    }
            }
        }.cache(1)
    }

    val markets: Flux<MarketData> = run {
        raw.ticker.map { tickers ->
            var marketIntStringMap: Map<MarketId, Market> = HashMap.empty()
            var marketStringIntMap: Map<Market, MarketId> = HashMap.empty()

            // TODO: Review frozen filter
            tickers.iterator().filter { !it._2.isFrozen }.forEach { tick ->
                marketIntStringMap = marketIntStringMap.put(tick._2.id, tick._1)
                marketStringIntMap = marketStringIntMap.put(tick._1, tick._2.id)
            }

            Tuple2(marketIntStringMap, marketStringIntMap)
        }.doOnNext { markets ->
            logger.info("Markets fetched: ${markets._2.size()}")
            if (logger.isDebugEnabled) logger.debug(markets.toString())
        }.cache(1)
    }

    val tradesStat: Flux<Map<MarketId, Flux<TradeStat>>> = run {
        val bufferLimit = 100

        markets.map { (_, marketStringMap) ->
            marketStringMap.map { market, marketId ->
                val initialTrades =
                    poloniexApi.tradeHistoryPublic(market).flux().doOnNext {
                        if (logger.isDebugEnabled) logger.debug("Received initial trades for $market")
                    }.doOnError {
                        if (logger.isDebugEnabled) logger.debug("Error received for initial trades for $market")
                    } // TODO: Specify backoff

                val tradesStream = orderBooks.map { it.get(marketId).get() }
                    .switchMap { it }
                    .map { it.notification }
                    .filter { it is OrderBookTrade }
                    .map { it as OrderBookTrade }

                val initialTrades0 = initialTrades.map { allTrades ->
                    var sellTrades = Queue.empty<TradeStatModels.SimpleTrade>()
                    var buyTrades = Queue.empty<TradeStatModels.SimpleTrade>()

                    for (trade in allTrades) {
                        val trade0 = TradeStatModels.SimpleTrade(
                            trade.price, trade.amount, trade.date.toInstant(
                                ZoneOffset.UTC
                            )
                        )

                        if (trade.type == OrderType.Sell) {
                            sellTrades = sellTrades.append(trade0)
                        } else {
                            buyTrades = buyTrades.append(trade0)
                        }
                    }

                    if (sellTrades.length() > bufferLimit) sellTrades =
                        sellTrades.dropRight(sellTrades.length() - bufferLimit)
                    if (buyTrades.length() > bufferLimit) buyTrades =
                        buyTrades.dropRight(buyTrades.length() - bufferLimit)

                    TradeStatModels.Trade0(sellTrades, buyTrades)
                }

                val trades1 = initialTrades0.map { allTrades ->
                    TradeStatModels.Trade1(
                        sellOld = allTrades.sell,
                        sellNew = allTrades.sell,
                        buyOld = allTrades.buy,
                        buyNew = allTrades.buy,
                        sellStatus = TradeStatModels.Trade1Status.Init,
                        buyStatus = TradeStatModels.Trade1Status.Init
                    )
                }.switchMap { initTrade1 ->
                    tradesStream.scan(
                        initTrade1
                    ) { trade1, bookTrade ->
                        val newTrade =
                            TradeStatModels.SimpleTrade(bookTrade.price, bookTrade.amount, bookTrade.timestamp)
                        val sellOld: Queue<TradeStatModels.SimpleTrade>
                        val sellNew: Queue<TradeStatModels.SimpleTrade>
                        val buyOld: Queue<TradeStatModels.SimpleTrade>
                        val buyNew: Queue<TradeStatModels.SimpleTrade>
                        val sellStatus: TradeStatModels.Trade1Status
                        val buyStatus: TradeStatModels.Trade1Status

                        if (bookTrade.orderType == OrderType.Sell) {
                            sellOld = trade1.sellNew
                            sellNew = TradeStatModels.Trade1.newTrades(newTrade, trade1.sellNew, bufferLimit)
                            buyOld = trade1.buyOld
                            buyNew = trade1.buyNew
                            sellStatus = TradeStatModels.Trade1Status.Changed
                            buyStatus = TradeStatModels.Trade1Status.NotChanged
                        } else {
                            buyOld = trade1.buyNew
                            buyNew = TradeStatModels.Trade1.newTrades(newTrade, trade1.buyNew, bufferLimit)
                            sellOld = trade1.sellOld
                            sellNew = trade1.sellNew
                            buyStatus = TradeStatModels.Trade1Status.Changed
                            sellStatus = TradeStatModels.Trade1Status.NotChanged
                        }

                        TradeStatModels.Trade1(sellOld, sellNew, buyOld, buyNew, sellStatus, buyStatus)
                    }
                }

                val trades2 = trades1.scan(
                    TradeStatModels.Trade2.DEFAULT
                ) { trade2, trade1 ->
                    val sell = when (trade1.sellStatus) {
                        TradeStatModels.Trade1Status.Changed -> TradeStatModels.Trade2State.calc(
                            trade2.sell,
                            trade1.sellOld,
                            trade1.sellNew
                        )
                        TradeStatModels.Trade1Status.NotChanged -> trade2.sell
                        TradeStatModels.Trade1Status.Init -> TradeStatModels.Trade2State.calcFull(trade1.sellNew)
                    }

                    val buy = when (trade1.buyStatus) {
                        TradeStatModels.Trade1Status.Changed -> TradeStatModels.Trade2State.calc(
                            trade2.buy,
                            trade1.buyOld,
                            trade1.buyNew
                        )
                        TradeStatModels.Trade1Status.NotChanged -> trade2.buy
                        TradeStatModels.Trade1Status.Init -> TradeStatModels.Trade2State.calcFull(trade1.buyNew)
                    }

                    TradeStatModels.Trade2(sell, buy)
                }.skip(1)

                val statStream = trades2.map { state ->
                    TradeStat(
                        sell = TradeStatModels.Trade2State.map(state.sell),
                        buy = TradeStatModels.Trade2State.map(state.buy)
                    )
                }.replay(1).refCount()

                Tuple2(marketId, statStream)
            }
        }.cache(1)
    }

    val openOrders: Flux<Map<Long, Trader.Companion.OpenOrder>> = run {
        val initialOrdersStream = raw.openOrders.map { marketOrdersMap ->
            marketOrdersMap.flatMap { (market, ordersSet) ->
                ordersSet.map { Trader.Companion.OpenOrder(it.orderNumber, it.type, market, it.price, it.amount) }
            }.map { order ->
                Tuple2(order.id, order)
            }.toMap { it }
        }

        val limitOrderCreatedStream = poloniexApi.accountNotificationStream
            .filter { it is LimitOrderCreated }
            .map { it as LimitOrderCreated }

        val orderUpdateStream = poloniexApi.accountNotificationStream
            .filter { it is OrderUpdate }
            .map { it as OrderUpdate }

        val orderUpdates = Flux.merge<AccountNotification>(limitOrderCreatedStream, orderUpdateStream)

        Flux.combineLatest<Map<Long, Trader.Companion.OpenOrder>, MarketData, Flux<Map<Long, Trader.Companion.OpenOrder>>>(
            initialOrdersStream,
            markets,
            BiFunction { initOrders: Map<Long, Trader.Companion.OpenOrder>, marketsInfo: MarketData ->
                orderUpdates.scan(
                    initOrders
                ) { orders: Map<Long, Trader.Companion.OpenOrder>, update: AccountNotification ->
                    when (update) {
                        is LimitOrderCreated -> run {
                            logger.info("Account info: $update")

                            val marketId = marketsInfo._1.get(update.marketId)

                            if (marketId.isDefined) {
                                val newOrder = Trader.Companion.OpenOrder(
                                    update.orderNumber,
                                    update.orderType,
                                    marketId.get(),
                                    update.rate,
                                    update.amount
                                )

                                orders.put(newOrder.id, newOrder)
                            } else {
                                logger.warn("Market id not found in local cache. Fetching markets from API...")
                                trigger.ticker.onNext(Unit)
                                orders
                            }
                        }
                        is OrderUpdate -> run {
                            logger.info("Account info: $update")

                            if (update.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                orders.remove(update.orderId)
                            } else {
                                val order = orders.get(update.orderId)

                                if (order.isDefined) {
                                    val oldOrder = order.get()
                                    val newOrder = Trader.Companion.OpenOrder(
                                        oldOrder.id,
                                        oldOrder.tpe,
                                        oldOrder.market,
                                        oldOrder.rate,
                                        update.newAmount
                                    )

                                    orders.put(oldOrder.id, newOrder)
                                } else {
                                    logger.warn("Order not found in local cache. Fetch orders from the server.")
                                    trigger.openOrders.onNext(Unit)
                                    orders
                                }
                            }
                        }
                        else -> run {
                            logger.warn("Received not recognized order update event: $update")
                            orders
                        }
                    }
                }
            }).concatMap { it }.cache(1)
    }

    val tickers: Flux<Map<Market, Ticker>> = run {
        markets.switchMap { marketInfo ->
            raw.ticker.switchMap { allTickers ->
                val allTickers0 = allTickers.map { m, t ->
                    tuple(
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

                raw.tickerStream.scan(allTickers0) { tickers, ticker ->
                    val marketId = marketInfo._1.get(ticker.id)

                    if (marketId != null) {
                        logger.trace {
                            logger.trace(ticker.toString())
                        }
                        tickers.put(marketId.get(), ticker)
                    } else {
                        logger.warn("Market not found in local market cache. Updating market cache...")
                        trigger.ticker.onNext(Unit)
                        allTickers0
                    }
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

                Tuple2(marketId, newBookStream)
            }
        }.cache(1)
    }

    val fee: Flux<FeeMultiplier> = run {
        poloniexApi.feeInfo().map { fee ->
            FeeMultiplier(fee.makerFee.oneMinus, fee.takerFee.oneMinus)
        }.flux().doOnNext { fee ->
            logger.info("Fee fetched: $fee")
        }.cache(1)
    }
}