package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import io.vavr.Tuple2
import io.vavr.collection.Set
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.component3
import io.vavr.kotlin.tuple
import mu.KotlinLogging
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.math.BigDecimal
import java.time.Duration
import java.util.function.Function.identity

class Trader(private val poloniexApi: PoloniexApi) {
    private val logger = KotlinLogging.logger {}

    private val trigger = TriggerStreams()
    private val raw = RawStreams(trigger, poloniexApi)
    val data = DataStreams(raw, trigger, poloniexApi)
    val indicators = IndicatorStreams(data)

    fun start(): Flux<Unit> {
        logger.info("Start trading")

        return Flux.just(Unit)
    }

    fun buySellInstantly(
        orderType: OrderType,
        market: Market,
        amount: Amount,
        fee: FeeMultiplier,
        orderBook: OrderBookAbstract
    ): Mono<BuySell> {
        val instantOrderSimulation =
            Orders.getInstantOrder(market, market.targetCurrency(orderType), amount, fee.taker, orderBook)
                ?: return Mono.error(Exception("Can't calculate simulation"))

        val price = when (orderType) {
            OrderType.Buy -> instantOrderSimulation.trades.iterator().map { it.price }.max()
            OrderType.Sell -> instantOrderSimulation.trades.iterator().map { it.price }.min()
        }

        if (price.isEmpty) return Mono.error(Exception("Simulation trades are empty"))

        return when (orderType) {
            OrderType.Buy -> poloniexApi.buy(market, price.get(), amount, BuyOrderType.FillOrKill)
            OrderType.Sell -> poloniexApi.sell(market, price.get(), amount, BuyOrderType.FillOrKill)
        }
    }

    fun buySellDelayed(
        orderType: OrderType,
        market: Market,
        amount: Flux<Amount>,
        orderBook: Flux<OrderBookAbstract>,
        threshold: Flux<BigDecimal>,
        minThreshold: BigDecimal
    ): Flux<TradeNotification> {
        val latestOrderIdsCount = 16
        var latestOrderIdsStream: Flux<Long> = Flux.empty()
        fun latestOrderIds(): Flux<Long> = latestOrderIdsStream

        val amountStream = amount.publish().refCount(2)

        val thresholdStream = threshold
            .filter { it < minThreshold }
            .take(1)
            .map { throw ThresholdException }
            .share()

        val tradeMatchesOrderId = { trade: AccountNotification ->
            if (trade is TradeNotification) {
                latestOrderIds().take(latestOrderIdsCount.toLong()).any { it == trade.orderId }
            } else {
                Mono.just(false)
            }
        }

        val unfilledAmount = Flux.merge(
            Int.MAX_VALUE,
            amountStream,
            raw.accountNotifications
                .filterWhen(tradeMatchesOrderId)
                .map {
                    val trade = it as TradeNotification
                    -trade.amount
                }
        )
            .scan(BigDecimal.ZERO) { state, delta -> state + delta }
            .skip(1)
            .replay(1)
            .refCount(1, Duration.ofNanos(1)) // TODO: Workaround for CancellationException

        val placeOrder = Mono.zip(
            orderBook.take(1).single(),
            data.orderBookOrders.take(1).single(),
            unfilledAmount.take(1).single()
        )
            .takeUntilOther(thresholdStream)
            .flatMap { values ->
                val book = values.t1
                val bookOrders = values.t2
                val amountValue = values.t3

                val price = when (orderType) {
                    OrderType.Buy -> run {
                        val bid = book.bids.headOption().map { it._1 }.orNull ?: return@run null

                        val anotherWorkerOnTheSameMarket = bookOrders.contains(BookOrder(market, bid, orderType))

                        if (anotherWorkerOnTheSameMarket) {
                            bid
                        } else {
                            val newPrice = bid.cut8add1
                            val ask = book.asks.headOption().map { it._1 }.orNull
                            if (ask != null && ask.compareTo(newPrice) == 0) bid else newPrice
                        }
                    }
                    OrderType.Sell -> run {
                        val ask = book.asks.headOption().map { it._1 }.orNull ?: return@run null

                        val anotherWorkerOnTheSameMarket = bookOrders.contains(BookOrder(market, ask, orderType))

                        if (anotherWorkerOnTheSameMarket) {
                            ask
                        } else {
                            val newPrice = ask.cut8minus1
                            val bid = book.bids.headOption().map { it._1 }.orNull
                            if (bid != null && bid.compareTo(newPrice) == 0) ask else newPrice
                        }
                    }
                } ?: return@flatMap Mono.error<Tuple2<Long, Price>>(OrderBookEmptyException(orderType))

                val placeOrderResult = when (orderType) {
                    OrderType.Buy -> poloniexApi.buy(market, price, amountValue, BuyOrderType.PostOnly)
                    OrderType.Sell -> poloniexApi.sell(market, price, amountValue, BuyOrderType.PostOnly)
                }

                placeOrderResult.map { result ->
                    tuple(result.orderId, price)
                }
            }

        val moveOrder = Flux.combineLatest(
            mutableListOf(
                orderBook,
                unfilledAmount,
                data.orderBookOrders
            ), Int.MAX_VALUE
        ) { values ->
            val book = values[0] as OrderBookAbstract
            val amountValue = values[1] as Amount

            @Suppress("UNCHECKED_CAST")
            val bookOrders = values[2] as Set<BookOrder>

            tuple(book, amountValue, bookOrders)
        }
            .onBackpressureLatest()
            .publishOn(Schedulers.parallel(), 1)
            .takeUntilOther(thresholdStream)
            .takeWhile { values ->
                val unfilledAmountValue = values._2
                unfilledAmountValue.compareTo(BigDecimal.ZERO) != 0
            }
            .scan(placeOrder.cache()) { state, values ->
                state.flatMap { stateValue ->
                    val (prevOrderId, delayedOrderPrice) = stateValue
                    val (book, unfilledAmountValue, bookOrders) = values

                    val newPrice: Price? = when (orderType) {
                        OrderType.Buy -> run {
                            val bid = book.bids.headOption().map { it._1 }.orNull
                                ?: return@flatMap Mono.error<Tuple2<Long, Price>>(OrderBookEmptyException(orderType))

                            if (bid > delayedOrderPrice) {
                                val anotherWorkerOnTheSameMarket =
                                    bookOrders.contains(BookOrder(market, bid, orderType))

                                if (anotherWorkerOnTheSameMarket) {
                                    bid
                                } else {
                                    var price = bid.cut8add1
                                    val ask = book.asks.headOption().map { it._1 }.orNull
                                    if (ask != null && ask.compareTo(price) == 0) price = bid
                                    price
                                }
                            } else {
                                null
                            }
                        }
                        OrderType.Sell -> run {
                            val ask = book.asks.headOption().map { it._1 }.orNull
                                ?: return@flatMap Mono.error<Tuple2<Long, Price>>(OrderBookEmptyException(orderType))

                            if (ask < delayedOrderPrice) {
                                val anotherWorkerOnTheSameMarket =
                                    bookOrders.contains(BookOrder(market, ask, orderType))

                                if (anotherWorkerOnTheSameMarket) {
                                    ask
                                } else {
                                    var price = ask.cut8minus1
                                    val bid = book.bids.headOption().map { it._1 }.orNull
                                    if (bid != null && bid.compareTo(price) == 0) price = ask
                                    price
                                }
                            } else {
                                null
                            }
                        }
                    }

                    if (newPrice != null) {
                        poloniexApi.moveOrder(
                            prevOrderId,
                            newPrice,
                            unfilledAmountValue,
                            BuyOrderType.PostOnly
                        )
                            .filterWhen { moveOrderResult ->
                                if (moveOrderResult.success) {
                                    Flux.just(true)
                                } else {
                                    Flux.error(Exception(/*moveOrderResult.message*/""))
                                }
                            }
                            .map { moveOrderResult ->
                                tuple(moveOrderResult.orderId, newPrice)
                            }
                    } else {
                        Mono.just(stateValue)
                    }
                }.cache()
            }
            .onBackpressureBuffer(Int.MAX_VALUE)
            .flatMapSequential(identity(), 1, 1)
            .distinctUntilChanged { it._1 }

        latestOrderIdsStream = moveOrder
            .map { it._1 }
            .replay(latestOrderIdsCount)
            .refCount(1, Duration.ofNanos(1)) // TODO: Workaround for CancellationException

        return raw.accountNotifications
            .filterWhen(tradeMatchesOrderId)
            .map { it as TradeNotification }
            .takeUntilOther(
                amountStream.ignoreElements().then(
                    unfilledAmount
                        .filter { it.compareTo(BigDecimal.ZERO) == 0 }
                        .take(1)
                        .single()
                )
            )
            .takeUntilOther(
                latestOrderIds()
                    .takeUntilOther(unfilledAmount.filter { it.compareTo(BigDecimal.ZERO) == 0 }.take(1))
                    .repeatWhen { unfilledAmount.onBackpressureLatest().filter { it > BigDecimal.ZERO } }
                    .ignoreElements()
            )
    }

    class OrderBookEmptyException(orderType: OrderType) :
        Throwable("Order book empty for order type $orderType", null, true, false)

    object ThresholdException : Throwable("Threshold Exception", null, true, false)
}
