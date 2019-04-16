package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import io.vavr.Tuple2
import io.vavr.collection.Queue
import io.vavr.kotlin.tuple
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ConflatedBroadcastChannel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.consumeEach
import kotlinx.coroutines.reactor.flux
import mu.KotlinLogging
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.math.BigDecimal

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
    ): Flux<TradeNotification> = GlobalScope.flux {
        val tradesChannel: ProducerScope<TradeNotification> = this
        val amountDelta = Channel<Amount>(Channel.UNLIMITED)
        val unfilledAmount = ConflatedBroadcastChannel<Amount>()
        val latestOrderIdsChannel = ConflatedBroadcastChannel<Queue<Long>>()

        // Input money monitoring
        val amountJob = launch {
            if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Start amount consumption...")

            try {
                amount.consumeEach { amount ->
                    if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Amount received: $amount")
                    amountDelta.send(amount)
                }

                if (logger.isDebugEnabled) logger.debug("[$market, $orderType] All amounts received")
            } catch (e: Throwable) {
                logger.error("Amount supplier completed with error for [$market, $orderType]", e)
            }
        }

        // Trades monitoring
        launch {
            if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Starting trades consumption...")

            raw.accountNotifications.consumeEach { trade ->
                if (trade !is TradeNotification) return@consumeEach

                if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Trade received: $trade")

                val orderIds = latestOrderIdsChannel.valueOrNull ?: return@consumeEach

                for (orderId in orderIds.reverseIterator()) {
                    if (orderId != trade.orderId) continue

                    if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Trade matched. Sending trade to the client..")

                    tradesChannel.send(trade)
                    amountDelta.send(-trade.amount)

                    break
                }
            }
        }

        // Threshold monitoring
        launch {
            if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Starting threshold job...")

            threshold.filter { it < minThreshold }.consumeEach {
                throw ThresholdException
            }
        }

        // Place - Move order loop
        launch {
            var amount0 = BigDecimal.ZERO
            var orderJob: Job? = null

            amountDelta.consumeEach {
                amount0 += it
                unfilledAmount.send(amount0)

                if (amount0.compareTo(BigDecimal.ZERO) == 0) {
                    if (orderJob != null) {
                        orderJob!!.cancelAndJoin()
                        orderJob = null
                    }
                } else {
                    if (orderJob == null) {
                        orderJob = launch {
                            val maxOrdersCount = 16
                            var latestOrderIds = Queue.empty<Long>()
                            var amountValue = unfilledAmount.value

                            if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Placing new order with amount $amountValue...")

                            val placeOrderResult = placeOrder(market, orderBook, orderType, amountValue)

                            var lastOrderId = placeOrderResult._1
                            var prevPrice = placeOrderResult._2

                            latestOrderIds = latestOrderIds.append(lastOrderId)
                            if (latestOrderIds.size() > maxOrdersCount) latestOrderIds = latestOrderIds.drop(1)
                            latestOrderIdsChannel.send(latestOrderIds)

                            if (logger.isDebugEnabled) logger.debug("[$market, $orderType] New order placed: $lastOrderId, $prevPrice")

                            orderBook.onBackpressureLatest().consumeEach orderBookLabel@{ book ->
                                if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Trying to move order $lastOrderId...")

                                amountValue = unfilledAmount.value
                                val moveOrderResult =
                                    moveOrder(market, book, orderType, lastOrderId, prevPrice, amountValue)
                                        ?: return@orderBookLabel

                                if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Order $lastOrderId moved to ${moveOrderResult._1} with price ${moveOrderResult._2}")

                                lastOrderId = moveOrderResult._1
                                prevPrice = moveOrderResult._2

                                latestOrderIds = latestOrderIds.append(lastOrderId)
                                if (latestOrderIds.size() > maxOrdersCount) latestOrderIds = latestOrderIds.drop(1)
                                latestOrderIdsChannel.send(latestOrderIds)
                            }
                        }
                    }
                }
            }
        }

        // Completion monitoring
        launch {
            if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Start monitoring for all trades completion...")

            amountJob.join()

            unfilledAmount.consumeEach {
                if (it.compareTo(BigDecimal.ZERO) == 0) {
                    if (logger.isDebugEnabled) logger.debug("[$market, $orderType] Amount = 0 => all trades received. Closing...")
                    tradesChannel.close()
                    return@launch
                }
            }
        }
    }

    private suspend fun moveOrder(
        market: Market,
        book: OrderBookAbstract,
        orderType: OrderType,
        lastOrderId: Long,
        prevPrice: Price,
        amountValue: BigDecimal?
    ): Tuple2<Long, Price>? {
        val newPrice = when (orderType) {
            OrderType.Buy -> run {
                val bid = book.bids.headOption().map { it._1 }.orNull
                    ?: throw OrderBookEmptyException(orderType)

                if (bid > prevPrice) {
                    val anotherWorkerOnTheSameMarket = data.orderBookOrders.awaitFirst()
                        .contains(BookOrder(market, bid, orderType))

                    if (anotherWorkerOnTheSameMarket) {
                        bid
                    } else {
                        var price = bid.cut8add1
                        val ask = book.asks.headOption().map { it._1 }.orNull
                        if (ask != null && ask.compareTo(price) == 0) price = bid
                        price
                    }
                } else {
                    val bid2 = book.bids.drop(1).headOption().map { it._1 }.orNull

                    if (bid2 == null) {
                        null
                    } else {
                        val newPrice = bid2.cut8add1

                        if (newPrice.compareTo(prevPrice) == 0) {
                            null
                        } else {
                            val anotherWorkerOnTheSameMarket = data.orderBookOrders.awaitFirst()
                                .contains(BookOrder(market, bid2, orderType))

                            if (anotherWorkerOnTheSameMarket) {
                                bid2
                            } else {
                                newPrice
                            }
                        }
                    }
                }
            }
            OrderType.Sell -> run {
                val ask = book.asks.headOption().map { it._1 }.orNull ?: throw OrderBookEmptyException(orderType)

                if (ask < prevPrice) {
                    val anotherWorkerOnTheSameMarket = data.orderBookOrders.awaitFirst()
                        .contains(BookOrder(market, ask, orderType))

                    if (anotherWorkerOnTheSameMarket) {
                        ask
                    } else {
                        var price = ask.cut8minus1
                        val bid = book.bids.headOption().map { it._1 }.orNull
                        if (bid != null && bid.compareTo(price) == 0) price = ask
                        price
                    }
                } else {
                    val ask2 = book.asks.drop(1).headOption().map { it._1 }.orNull

                    if (ask2 == null) {
                        null
                    } else {
                        val newPrice = ask2.cut8minus1

                        if (newPrice.compareTo(prevPrice) == 0) {
                            null
                        } else {
                            val anotherWorkerOnTheSameMarket = data.orderBookOrders.awaitFirst()
                                .contains(BookOrder(market, ask2, orderType))

                            if (anotherWorkerOnTheSameMarket) {
                                ask2
                            } else {
                                newPrice
                            }
                        }
                    }
                }
            }
        } ?: return null

        val moveOrderResult = poloniexApi.moveOrder(
            lastOrderId,
            newPrice,
            amountValue,
            BuyOrderType.PostOnly
        ).awaitSingle()

        if (moveOrderResult.success) {
            return tuple(moveOrderResult.orderId, newPrice)
        } else {
            throw Exception("Can't move order because move returned false")
        }
    }

    private suspend fun placeOrder(
        market: Market,
        orderBook: Flux<OrderBookAbstract>,
        orderType: OrderType,
        amountValue: BigDecimal
    ): Tuple2<Long, Price> {
        val book = orderBook.awaitFirst()
        val bookOrders = data.orderBookOrders.awaitFirst()

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
        } ?: throw OrderBookEmptyException(orderType)

        val result = when (orderType) {
            OrderType.Buy -> poloniexApi.buy(market, price, amountValue, BuyOrderType.PostOnly)
            OrderType.Sell -> poloniexApi.sell(market, price, amountValue, BuyOrderType.PostOnly)
        }.awaitSingle()

        return tuple(result.orderId, price)
    }

    class OrderBookEmptyException(orderType: OrderType) :
        Throwable("Order book empty for order type $orderType", null, true, false)

    object ThresholdException : Throwable("Threshold Exception", null, true, false)
}
