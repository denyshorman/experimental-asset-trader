package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.InvalidOrderNumberException
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.TransactionFailedException
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.util.FlowScope
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
import java.math.BigDecimal

class Trader(private val poloniexApi: PoloniexApi) {
    private val logger = KotlinLogging.logger {}

    private val trigger = TriggerStreams()
    private val raw = RawStreams(trigger, poloniexApi)
    val data = DataStreams(raw, trigger, poloniexApi)
    val indicators = IndicatorStreams(data)

    fun start(): Flux<Unit> {
        return FlowScope.flux {
            logger.info("Start trading")

            val market = Market("USDT", "MANA")
            val marketId = 231
            val amount = Flux.just(BigDecimal("20.9832"))
            val threshold = Flux.never<BigDecimal>()
            val minThreshold = BigDecimal.ONE
            val orderBook = data.orderBooks.awaitFirst()[marketId].get().map { it.book as OrderBookAbstract }

            buySellDelayed(OrderType.Sell, market, amount, orderBook, threshold, minThreshold).consumeEach {
                logger.info("trade received")
            }

            channel.close()
        }
    }

    fun buySellInstantly(
        orderType: OrderType,
        market: Market,
        amountFlow: Flux<Amount>,
        orderBookFlow: Flux<OrderBookAbstract>,
        feeFlow: Flux<FeeMultiplier>,
        threshold: Flux<BigDecimal>,
        minThreshold: BigDecimal
    ): Flux<BuyResultingTrade> = FlowScope.flux {
        amountFlow.onBackpressureBuffer().consumeEach { amount ->
            val fee = feeFlow.awaitFirst()
            val orderBook = orderBookFlow.awaitFirst()

            val instantOrderSimulation =
                Orders.getInstantOrder(market, market.targetCurrency(orderType), amount, fee.taker, orderBook)
                    ?: throw Exception("Can't calculate simulation")

            val price = when (orderType) {
                OrderType.Buy -> instantOrderSimulation.trades.iterator().map { it.price }.max()
                OrderType.Sell -> instantOrderSimulation.trades.iterator().map { it.price }.min()
            }

            if (price.isEmpty) throw Exception("Simulation trades are empty")

            val orderResult = when (orderType) {
                OrderType.Buy -> poloniexApi.buy(market, price.get(), amount, BuyOrderType.FillOrKill)
                OrderType.Sell -> poloniexApi.sell(market, price.get(), amount, BuyOrderType.FillOrKill)
            }.awaitSingle()

            orderResult.resultingTrades.forEach { trade ->
                channel.send(trade)
            }
        }

        channel.close()
    }

    fun buySellDelayed(
        orderType: OrderType,
        market: Market,
        amount: Flux<Amount>,
        orderBook: Flux<OrderBookAbstract>,
        threshold: Flux<BigDecimal>,
        minThreshold: BigDecimal
    ): Flux<TradeNotification> = FlowScope.flux {
        val tradesChannel: ProducerScope<TradeNotification> = this
        val amountDelta = Channel<Amount>(Channel.UNLIMITED)
        val unfilledAmount = ConflatedBroadcastChannel<Amount>()
        val latestOrderIdsChannel = ConflatedBroadcastChannel<Queue<Long>>()

        // Input money monitoring
        val amountJob = launch {
            if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Start amount consumption...")

            try {
                amount.consumeEach { amount ->
                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Amount received: $amount")
                    amountDelta.send(amount)
                }

                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] All amounts received")
            } catch (e: Throwable) {
                logger.error("Amount supplier completed with error for [$market, $orderType]", e)
            }
        }

        // Trades monitoring
        launch {
            if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Starting trades consumption...")

            raw.accountNotifications.consumeEach { trade ->
                if (trade !is TradeNotification) return@consumeEach

                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trade received: $trade")

                val orderIds = latestOrderIdsChannel.valueOrNull ?: return@consumeEach

                for (orderId in orderIds.reverseIterator()) {
                    if (orderId != trade.orderId) continue

                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trade matched. Sending trade to the client..")

                    tradesChannel.send(trade)
                    amountDelta.send(-trade.amount)

                    break
                }
            }
        }

        // Threshold monitoring
        launch {
            if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Starting threshold job...")

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

                            if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Placing new order with amount $amountValue...")

                            val placeOrderResult = placeOrder(market, orderBook, orderType, amountValue)

                            var lastOrderId = placeOrderResult._1
                            var prevPrice = placeOrderResult._2

                            latestOrderIds = latestOrderIds.append(lastOrderId)
                            if (latestOrderIds.size() > maxOrdersCount) latestOrderIds = latestOrderIds.drop(1)
                            latestOrderIdsChannel.send(latestOrderIds)

                            if (logger.isTraceEnabled) logger.trace("[$market, $orderType] New order placed: $lastOrderId, $prevPrice")

                            orderBook.onBackpressureLatest().consumeEach orderBookLabel@{ book ->
                                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Trying to move order $lastOrderId...")

                                amountValue = unfilledAmount.value

                                var moveOrderResult: Tuple2<Long, Price>? = null

                                while (isActive) {
                                    try {
                                        moveOrderResult =
                                            moveOrder(market, book, orderType, lastOrderId, prevPrice, amountValue)
                                                ?: return@orderBookLabel

                                        break
                                    } catch (e: TransactionFailedException) {
                                        continue
                                    } catch (e: InvalidOrderNumberException) {
                                        cancel()
                                    }
                                }

                                if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Order $lastOrderId moved to ${moveOrderResult!!._1} with price ${moveOrderResult._2}")

                                lastOrderId = moveOrderResult!!._1
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
            if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Start monitoring for all trades completion...")

            amountJob.join()

            unfilledAmount.consumeEach {
                if (it.compareTo(BigDecimal.ZERO) == 0) {
                    if (logger.isTraceEnabled) logger.trace("[$market, $orderType] Amount = 0 => all trades received. Closing...")
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
        myPrice: Price,
        amountValue: BigDecimal
    ): Tuple2<Long, Price>? {
        val newPrice = run {
            val primaryBook: SubOrderBook
            val secondaryBook: SubOrderBook
            val priceComparator: Comparator<Price>
            val moveToOnePoint: (Price) -> Price

            when (orderType) {
                OrderType.Buy -> run {
                    primaryBook = book.bids
                    secondaryBook = book.asks
                    priceComparator = Comparator { bookPrice, myPrice ->
                        when {
                            bookPrice > myPrice -> 1
                            bookPrice.compareTo(myPrice) == 0 -> 0
                            else -> -1
                        }
                    }
                    moveToOnePoint = { price -> price.cut8add1 }
                }
                OrderType.Sell -> run {
                    primaryBook = book.asks
                    secondaryBook = book.bids
                    priceComparator = Comparator { bookPrice, myPrice ->
                        when {
                            bookPrice < myPrice -> 1
                            bookPrice.compareTo(myPrice) == 0 -> 0
                            else -> -1
                        }
                    }
                    moveToOnePoint = { price -> price.cut8minus1 }
                }
            }

            val bookPrice1 = primaryBook.headOption().map { it._1 }.orNull ?: throw OrderBookEmptyException(orderType)
            val myPositionInBook = priceComparator.compare(bookPrice1, myPrice)

            if (myPositionInBook == 1) {
                // I am on second position
                val anotherWorkerOnTheSameMarket = data.orderBookOrders.awaitFirst()
                    .contains(BookOrder(market, bookPrice1, orderType))

                if (anotherWorkerOnTheSameMarket) {
                    bookPrice1
                } else {
                    var price = moveToOnePoint(bookPrice1)
                    val ask = secondaryBook.headOption().map { it._1 }.orNull
                    if (ask != null && ask.compareTo(price) == 0) price = bookPrice1
                    price
                }
            } else {
                // I am on first position
                null // Ignore possible price gaps
            }
        } ?: return null

        val moveOrderResult = poloniexApi.moveOrder(
            lastOrderId,
            newPrice,
            amountValue,
            BuyOrderType.PostOnly
        ).awaitSingle()

        return tuple(moveOrderResult.orderId, newPrice)
    }

    private suspend fun placeOrder(
        market: Market,
        orderBook: Flux<OrderBookAbstract>,
        orderType: OrderType,
        amountValue: BigDecimal
    ): Tuple2<Long, Price> {
        val primaryBook: SubOrderBook
        val secondaryBook: SubOrderBook
        val moveToOnePoint: (Price) -> Price

        val book = orderBook.awaitFirst()
        val bookOrders = data.orderBookOrders.awaitFirst()

        when (orderType) {
            OrderType.Buy -> run {
                primaryBook = book.bids
                secondaryBook = book.asks
                moveToOnePoint = { price -> price.cut8add1 }
            }
            OrderType.Sell -> run {
                primaryBook = book.asks
                secondaryBook = book.bids
                moveToOnePoint = { price -> price.cut8minus1 }
            }
        }

        val price = run {
            val topPricePrimary = primaryBook.headOption().map { it._1 }.orNull ?: return@run null

            val anotherWorkerOnTheSameMarket = bookOrders.contains(BookOrder(market, topPricePrimary, orderType))

            if (anotherWorkerOnTheSameMarket) {
                topPricePrimary
            } else {
                val newPrice = moveToOnePoint(topPricePrimary)
                val topPriceSecondary = secondaryBook.headOption().map { it._1 }.orNull
                if (topPriceSecondary != null && topPriceSecondary.compareTo(newPrice) == 0) topPricePrimary else newPrice
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
