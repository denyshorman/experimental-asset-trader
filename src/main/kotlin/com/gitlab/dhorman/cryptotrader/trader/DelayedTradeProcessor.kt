package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.trader.algo.SplitTradeAlgo
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.trader.exception.CantMoveOrderSafelyException
import com.gitlab.dhorman.cryptotrader.trader.exception.CompletePlaceMoveOrderLoop
import com.gitlab.dhorman.cryptotrader.trader.exception.MoveNotRequiredException
import com.gitlab.dhorman.cryptotrader.trader.exception.RepeatPlaceMoveOrderLoopAgain
import com.gitlab.dhorman.cryptotrader.util.returnLastIfNoValueWithinSpecifiedTime
import io.vavr.Tuple2
import io.vavr.collection.List
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.tuple
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import java.io.IOException
import java.math.BigDecimal
import java.net.ConnectException
import java.net.SocketException
import java.net.UnknownHostException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.Comparator

class DelayedTradeProcessor(
    val market: Market,
    val orderType: OrderType,
    private val orderBook: Flow<OrderBookAbstract>,
    private val scope: CoroutineScope,
    splitAlgo: SplitTradeAlgo,
    private val poloniexApi: ExtendedPoloniexApi,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator,
    private val transactionsDao: TransactionsDao,
    private val clock: Clock
) {
    private val scheduler = TradeScheduler(orderType, splitAlgo, amountCalculator)
    private var tradeWorkerJob: Job? = null
    private val mutex = Mutex()

    @Volatile
    private var lastOrder: Order? = null

    @Volatile
    private var lastError: Throwable? = null

    fun start(): Job = scope.launch(start = CoroutineStart.UNDISPATCHED) {
        try {
            delay(Long.MAX_VALUE)
        } finally {
            withContext(NonCancellable) {
                mutex.withLock {
                    stopTradeWorker()
                    scheduler.unregisterAll()
                }
            }
        }
    }

    suspend fun register(id: PathId, outputTrades: Channel<List<BareTrade>>) {
        mutex.withLock {
            scheduler.register(id, outputTrades)
        }
    }

    suspend fun unregister(id: PathId) {
        mutex.withLock {
            if (!scheduler.pathExists(id)) {
                logger.debug { "Unregister is not required because path has already been removed" }
                return
            }

            stopTradeWorker()
            scheduler.unregister(id)
            startTradeWorker()
        }
    }

    suspend fun addAmount(id: PathId, fromAmount: Amount): Boolean {
        return mutex.withLock {
            stopTradeWorker()
            val amountAdded = scheduler.addAmount(id, fromAmount)
            startTradeWorker()
            amountAdded
        }
    }

    suspend fun pause() {
        logger.debug("Trying to pause Delayed Trade Processor..")
        mutex.lock()
        stopTradeWorker()
    }

    fun resume() {
        startTradeWorker()
        mutex.unlock()
        logger.debug("Delayed Trade Processor has been successfully resumed")
    }

    private suspend fun stopTradeWorker() {
        logger.debug { "Trying to stop Delayed Trade Processor..." }
        tradeWorkerJob?.cancelAndJoin()
        tradeWorkerJob = null
        logger.debug { "Delayed Trade Processor has been stopped" }
    }

    private fun startTradeWorker() {
        if (tradeWorkerJob != null || scheduler.fromAmount.compareTo(BigDecimal.ZERO) == 0) return

        val workerJob = launchTradeWorker()
        tradeWorkerJob = workerJob
        scope.launch(Job(), CoroutineStart.UNDISPATCHED) {
            workerJob.join()
            logger.debug { "Delayed Trade Processor has been stopped" }

            mutex.withLock {
                if (tradeWorkerJob == workerJob) tradeWorkerJob = null
                startTradeWorker()
            }
        }
        logger.debug { "Delayed Trade Processor has been launched" }
    }

    private fun launchTradeWorker(): Job = scope.launch(Job()) {
        val thisJob = coroutineContext[Job]
        fun stopSelf() = thisJob?.cancel()
        fun stoppedSelf() = thisJob?.isCancelled == true

        val tradeMonitoringJob = launch(Job(), CoroutineStart.UNDISPATCHED) {
            poloniexApi.accountNotificationStream.collect { notifications ->
                var tradeList: LinkedList<BareTrade>? = null
                var tradeStatusList: LinkedList<Order.Trade>? = null
                var orderCancelledList: LinkedList<Order>? = null
                var orderCreatedList: LinkedList<Order>? = null

                withContext(NonCancellable) {
                    for (notification in notifications) {
                        when (notification) {
                            is TradeNotification -> run {
                                val order = if (notification.clientOrderId != null) lastOrder.getOrderWithClientOrderId(notification.clientOrderId) else null

                                if (order != null) {
                                    if (tradeList == null) {
                                        tradeList = LinkedList()
                                        tradeStatusList = LinkedList()
                                    }

                                    val tradeStatus = Order.Trade(notification.tradeId, processed = false)
                                    order.trades[notification.tradeId] = tradeStatus
                                    tradeStatusList!!.add(tradeStatus)
                                    tradeList!!.add(notification.toBareTrade())
                                }
                            }
                            is LimitOrderCreated -> run {
                                if (notification.clientOrderId != null) {
                                    val order = lastOrder.getOrderWithClientOrderId(notification.clientOrderId)
                                    if (order != null) {
                                        if (orderCreatedList == null) orderCreatedList = LinkedList()
                                        orderCreatedList!!.add(order)
                                    }
                                }
                            }
                            is OrderUpdate -> run {
                                if (notification.clientOrderId != null && notification.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                    val order = lastOrder.getOrderWithClientOrderId(notification.clientOrderId)
                                    if (order != null) {
                                        if (orderCancelledList == null) orderCancelledList = LinkedList()
                                        orderCancelledList!!.add(order)
                                    }
                                }
                            }
                            else -> run {
                                // ignore other events
                            }
                        }
                    }

                    if (tradeList != null && !tradeList!!.isEmpty()) {
                        scheduler.addTrades(tradeList!!)
                        tradeStatusList!!.forEach { it.processed = true } // TODO: Does not work well for blackout
                    }

                    orderCancelledList?.forEach { it.status.value = Order.Status.Cancelled }
                    orderCreatedList?.forEach { it.status.value = Order.Status.Created }
                }
            }
        }

        try {
            while (isActive) {
                lastOrder = null
                lastError = null

                logger.debug { "Start place-move order loop" }

                try {
                    coroutineScope placeMoveLoopBegin@{
                        launch(start = CoroutineStart.UNDISPATCHED) {
                            logger.debug { "Start connection monitoring" }
                            poloniexApi.channelStateFlow(DefaultChannel.AccountNotifications.id).filter { !it }.first()
                            throw DisconnectedException
                        }

                        while (isActive) {
                            lastOrder = null
                            try {
                                withContext(NonCancellable) {
                                    placeOrder()
                                    logger.debug { "Order ${lastOrder!!.orderId} has been placed (q = ${lastOrder!!.quoteAmount}, p = ${lastOrder!!.price})" }
                                }

                                try {
                                    withTimeout(OrderPlaceCancelConfirmationTimeoutMs) {
                                        lastOrder!!.awaitStatus(Order.Status.Created)
                                    }
                                } catch (e: TimeoutCancellationException) {
                                    logger.error("OrderPlaced notification has not been received within specified timeout")
                                    throw DisconnectedException
                                }

                                val timeout = Duration.ofSeconds(4)
                                val timeoutMillis = timeout.toMillis()
                                var bookUpdateTime = System.currentTimeMillis()
                                var bookChangeCounter = 0
                                var prevBook: OrderBookAbstract? = null

                                logger.debug { "Start market maker movement process" }

                                orderBook.returnLastIfNoValueWithinSpecifiedTime(timeout).conflate().collect moveOrderLoopBegin@{ book ->
                                    try {
                                        withContext(NonCancellable) {
                                            if (stoppedSelf()) return@withContext

                                            bookChangeCounter = if (bookChangeCounter >= 10) 0 else bookChangeCounter + 1
                                            val now = System.currentTimeMillis()
                                            val fixPriceGaps = now - bookUpdateTime >= timeoutMillis || bookChangeCounter == 0
                                            bookUpdateTime = now
                                            prevBook = book

                                            moveOrder(book, fixPriceGaps)

                                            logger.debug { "Moved order (q = ${lastOrder?.quoteAmount}, p = ${lastOrder?.price})" }
                                        }
                                    } catch (e: CantMoveOrderSafelyException) {
                                        withContext(NonCancellable) {
                                            cancelOrder(lastOrder!!.orderId!!, CancelOrderIdType.Server)
                                            try {
                                                withTimeout(OrderPlaceCancelConfirmationTimeoutMs) {
                                                    lastOrder!!.awaitStatus(Order.Status.Cancelled)
                                                }
                                                throw RepeatPlaceMoveOrderLoopAgain
                                            } catch (e: TimeoutCancellationException) {
                                                logger.error("Cancel event has not been received within specified timeout in order to place order again")
                                                throw DisconnectedException
                                            }
                                        }
                                    } catch (e: OrderBookEmptyException) {
                                        logger.warn("Can't move order: ${e.message}")
                                        return@moveOrderLoopBegin
                                    } catch (e: TransactionFailedException) {
                                        logger.debug(e.originalMsg)
                                        return@moveOrderLoopBegin
                                    } catch (e: MaxOrdersExceededException) {
                                        logger.debug(e.originalMsg)
                                        delay(1500)
                                        return@moveOrderLoopBegin
                                    } catch (e: MoveNotRequiredException) {
                                        return@moveOrderLoopBegin
                                    } catch (e: UnableToPlacePostOnlyOrderException) {
                                        logger.debug {
                                            val asks = prevBook?.asks?.take(3)?.joinToString(separator = ";") { "(${it._1},${it._2})" }
                                            val bids = prevBook?.bids?.take(3)?.joinToString(separator = ";") { "(${it._1},${it._2})" }
                                            "${e.message} ; book = (asks = $asks ; bids = $bids) ; currFromAmount = ${scheduler.fromAmount} ; $lastOrder"
                                        }
                                        return@moveOrderLoopBegin
                                    } catch (e: CancellationException) {
                                        throw e
                                    } catch (e: Throwable) {
                                        when (e) {
                                            is OrderCompletedOrNotExistException,
                                            is InvalidOrderNumberException,
                                            is NotEnoughCryptoException,
                                            is AmountMustBeAtLeastException,
                                            is TotalMustBeAtLeastException,
                                            is RateMustBeLessThanException -> run {
                                                logger.debug { "Tried to move order but error received: ${e.message} | $lastOrder" }
                                                try {
                                                    withTimeout(OrderPlaceCancelConfirmationTimeoutMs) {
                                                        lastOrder!!.prevOrder!!.awaitStatus(Order.Status.Cancelled)
                                                    }
                                                    throw RepeatPlaceMoveOrderLoopAgain
                                                } catch (e: TimeoutCancellationException) {
                                                    logger.error("Was waiting for order status cancel event but it has not received within specified timeout")
                                                    throw DisconnectedException
                                                }
                                            }
                                            else -> throw e
                                        }
                                    }

                                    try {
                                        withTimeout(OrderPlaceCancelConfirmationTimeoutMs) {
                                            lastOrder!!.prevOrder!!.awaitStatus(Order.Status.Cancelled)
                                            lastOrder!!.awaitStatus(Order.Status.Created)
                                        }
                                    } catch (e: TimeoutCancellationException) {
                                        logger.error("OrderPlaced and OrderCancelled notifications have not been received within specified timeout")
                                        throw DisconnectedException
                                    }
                                }
                            } catch (_: RepeatPlaceMoveOrderLoopAgain) {
                            }
                        }
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: CompletePlaceMoveOrderLoop) {
                } catch (e: DisconnectedException) {
                    logger.debug { "Disconnected from account notification channel" }
                    poloniexApi.channelStateFlow(DefaultChannel.AccountNotifications.id).filter { it }.first()
                    logger.debug { "Connected to account notification channel" }
                } catch (e: Throwable) {
                    lastError = e
                    logger.debug("Error occurred during place-move order loop ${lastError?.message}")
                } finally {
                    withContext(NonCancellable) {
                        try {
                            if (lastOrder == null) {
                                logger.debug { "Don't have any opened orders" }
                                return@withContext
                            }

                            val orders = lastOrder!!.asSequence().take(2).toList()

                            val orderCancelStatusesDeferred = orders.asSequence()
                                .filter { it.status.value != Order.Status.Cancelled }
                                .map { order ->
                                    async {
                                        val cancelStatus = cancelOrder(order.clientOrderId, CancelOrderIdType.Client)

                                        when (cancelStatus) {
                                            OrderCancelStatus.Cancelled -> {
                                                try {
                                                    withTimeout(OrderPlaceCancelConfirmationTimeoutMs) {
                                                        logger.debug { "Waiting for order ${order.clientOrderId} cancel confirmation..." }
                                                        order.awaitStatus(Order.Status.Cancelled)
                                                        logger.debug { "Order cancel confirmation event has been received for order ${order.clientOrderId}." }
                                                    }
                                                } catch (e: TimeoutCancellationException) {
                                                    logger.warn { "Cancel event has not been received within specified timeout for order ${order.clientOrderId}" }
                                                }
                                            }
                                            OrderCancelStatus.OrderNotExist -> {
                                                logger.debug { "Can't cancel order ${order.clientOrderId} because it does not exist" }
                                            }
                                        }

                                        cancelStatus
                                    }
                                }
                                .toList()

                            orderCancelStatusesDeferred.forEach { it.await() }

                            val missedTradesDeferred = orders.map { order ->
                                async {
                                    val processedTrades = order.trades.asSequence()
                                        .filter { it.value.processed }
                                        .map { it.key }
                                        .toHashSet()

                                    if (order.orderId != null) {
                                        logger.debug { "Trying to fetch missed trades for order ${order.orderId} and processed trades $processedTrades" }
                                        poloniexApi.missedTrades(order.orderId!!, processedTrades)
                                    } else {
                                        logger.debug {
                                            "Trying to fetch missed trades using workaround for order ${order.clientOrderId} " +
                                                "with time ${order.createTime}, " +
                                                "price ${order.price}, " +
                                                "processed trades $processedTrades"
                                        }
                                        poloniexApi.missedTrades(market, orderType, TradeCategory.Exchange, order.createTime, order.price, processedTrades)
                                    }
                                }
                            }

                            val missedTrades = missedTradesDeferred.flatMap { it.await() }

                            if (missedTrades.isNotEmpty()) {
                                logger.debug { "Adding missed trades: $missedTrades" }
                                scheduler.addTrades(missedTrades)
                            } else {
                                logger.debug("Missed trades not found")
                            }
                        } finally {
                            if (lastError != null) {
                                scheduler.unregisterAll(lastError)
                                stopSelf()
                            }
                        }
                    }
                }
            }
        } finally {
            withContext(NonCancellable) {
                tradeMonitoringJob.cancelAndJoin()
            }
        }
    }

    private suspend fun calcPlaceOrderAmountPrice(fromAmountValue: BigDecimal): Tuple2<BigDecimal, Price> {
        val primaryBook: SubOrderBook
        val secondaryBook: SubOrderBook
        val moveToOnePoint: (Price) -> Price

        val book = orderBook.first()

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
            val newPrice = moveToOnePoint(topPricePrimary)
            val topPriceSecondary = secondaryBook.headOption().map { it._1 }.orNull
            if (topPriceSecondary != null && topPriceSecondary.compareTo(newPrice) == 0) {
                topPricePrimary
            } else {
                newPrice
            }
        } ?: throw OrderBookEmptyException(orderType)

        val quoteAmount = when (orderType) {
            OrderType.Buy -> amountCalculator.quoteAmount(fromAmountValue, price, BigDecimal.ONE)
            OrderType.Sell -> fromAmountValue
        }

        return tuple(quoteAmount, price)
    }

    private fun calcMoveOrderAmountPrice(
        book: OrderBookAbstract,
        fromAmountValue: Amount,
        prevQuoteAmount: Amount,
        prevPrice: Price,
        fixPriceGaps: Boolean
    ): Price {
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

            val (bookPrice1, bookQuoteAmount1) = primaryBook.headOption().orNull ?: throw OrderBookEmptyException(orderType)
            val myPositionInBook = priceComparator.compare(bookPrice1, prevPrice)

            if (myPositionInBook == 1 || myPositionInBook == 0 && prevQuoteAmount < bookQuoteAmount1) {
                // I am on second position
                var price = moveToOnePoint(bookPrice1)
                val ask = secondaryBook.headOption().map { it._1 }.orNull
                if (ask != null && ask.compareTo(price) == 0) price = bookPrice1
                price
            } else {
                // I am on first position
                if (fixPriceGaps) {
                    val secondPrice = primaryBook.drop(1).headOption().map { moveToOnePoint(it._1) }.orNull
                    if (secondPrice != null && priceComparator.compare(prevPrice, secondPrice) == 1) secondPrice else null
                } else {
                    null
                }
            }
        } ?: throw MoveNotRequiredException

        if (orderType == OrderType.Buy) {
            val from = amountCalculator.fromAmountBuy(prevQuoteAmount, newPrice, BigDecimal.ONE)
            if (from > fromAmountValue) {
                logger.debug {
                    val bids = book.bids.take(3).joinToString(separator = ";") { "(${it._2},${it._1})" }
                    "Can't move order safely. $from ($prevQuoteAmount, $newPrice) > $fromAmountValue ($prevQuoteAmount, $prevPrice) | bids = $bids"
                }
                throw CantMoveOrderSafelyException
            }
        }

        return newPrice
    }

    private suspend fun placeOrder() {
        while (true) {
            try {
                val (quoteAmount, price) = calcPlaceOrderAmountPrice(scheduler.fromAmount)

                if (quoteAmount.compareTo(BigDecimal.ZERO) == 0) throw AmountIsZeroException

                val order = Order(
                    clientOrderId = poloniexApi.generateOrderId(),
                    quoteAmount = quoteAmount,
                    price = price,
                    createTime = Instant.now(clock),
                    prevOrder = lastOrder
                )

                lastOrder = order
                lastOrder.cleanUp()

                val result = poloniexApi.placeLimitOrder(
                    market,
                    orderType,
                    price,
                    quoteAmount,
                    BuyOrderType.PostOnly,
                    order.clientOrderId
                )

                order.orderId = result.orderId
                return
            } catch (e: Throwable) {
                when (e) {
                    is UnableToPlacePostOnlyOrderException,
                    is TransactionFailedException -> run {
                        logger.debug(e.message)
                        delay(100)
                    }

                    is OrderBookEmptyException,
                    is MaxOrdersExceededException -> run {
                        logger.debug(e.message)
                        delay(1000)
                    }

                    is UnknownHostException,
                    is IOException,
                    is ConnectException,
                    is SocketException -> throw DisconnectedException

                    else -> throw e
                }
            }
        }
    }

    private suspend fun moveOrder(book: OrderBookAbstract, fixPriceGaps: Boolean = false) {
        while (true) {
            try {
                val lastCreatedOrder = lastOrder.lastOrderWith(Order.Status.Created) ?: throw CompletePlaceMoveOrderLoop

                val price = calcMoveOrderAmountPrice(
                    book,
                    scheduler.fromAmount,
                    lastCreatedOrder.quoteAmount,
                    lastCreatedOrder.price,
                    fixPriceGaps
                )

                val newOrder = Order(
                    clientOrderId = poloniexApi.generateOrderId(),
                    quoteAmount = lastCreatedOrder.quoteAmount,
                    price = price,
                    createTime = Instant.now(clock),
                    prevOrder = lastCreatedOrder
                )

                lastOrder = newOrder
                lastOrder.cleanUp()

                val moveOrderResult = poloniexApi.moveOrder(
                    lastCreatedOrder.orderId!!,
                    newOrder.price,
                    null,
                    BuyOrderType.PostOnly,
                    newOrder.clientOrderId
                )

                newOrder.orderId = moveOrderResult.orderId
                return
            } catch (e: Throwable) {
                when (e) {
                    is UnknownHostException,
                    is IOException,
                    is ConnectException,
                    is SocketException -> throw DisconnectedException

                    else -> throw e
                }
            }
        }
    }

    private suspend fun cancelOrder(orderId: Long, orderIdType: CancelOrderIdType): OrderCancelStatus {
        while (true) {
            try {
                logger.debug { "Cancelling order $orderId" }
                poloniexApi.cancelOrder(orderId, orderIdType)
                logger.debug { "Order $orderId has been cancelled" }
                return OrderCancelStatus.Cancelled
            } catch (e: Throwable) {
                when (e) {
                    is CancellationException -> throw e
                    is OrderCompletedOrNotExistException -> run {
                        logger.debug(e.message)
                        return OrderCancelStatus.OrderNotExist
                    }
                    is AlreadyCalledCancelOrMoveOrderException -> run {
                        logger.warn { "Can't cancel order: ${e.message}" }
                        delay(1000)
                    }
                    is UnknownHostException,
                    is IOException,
                    is ConnectException,
                    is SocketException -> run {
                        logger.debug { "Cancel order $orderId: ${e.message}" }
                        delay(2000)
                    }
                    else -> run {
                        logger.error(e.message)
                        delay(2000)
                    }
                }
            }
        }
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        private const val OrderPlaceCancelConfirmationTimeoutMs = 60000L

        private fun TradeNotification.toBareTrade(): BareTrade {
            return BareTrade(this.amount, this.price, this.feeMultiplier)
        }

        private enum class OrderCancelStatus { Cancelled, OrderNotExist }

        private class Order(
            @Volatile var orderId: Long? = null,
            @Volatile var clientOrderId: Long,
            @Volatile var quoteAmount: Amount,
            @Volatile var price: Price,
            val status: MutableStateFlow<Status> = MutableStateFlow(Status.Pending),
            val trades: ConcurrentHashMap<Long, Trade> = ConcurrentHashMap(),
            val createTime: Instant,
            @Volatile var prevOrder: Order? = null
        ) {
            override fun toString(): String {
                return "Order(" +
                    "orderId=$orderId, " +
                    "clientOrderId=$clientOrderId, " +
                    "quoteAmount=$quoteAmount, " +
                    "price=$price, " +
                    "status=${status.value}, " +
                    "createTime=$createTime, " +
                    "trades=$trades, " +
                    "prevOrder=$prevOrder" +
                    ")"
            }

            enum class Status { Pending, Created, Cancelled }

            class Trade(
                val id: Long,
                @Volatile var processed: Boolean = false
            ) {
                override fun toString(): String {
                    return "Trade(id=$id, processed=$processed)"
                }
            }
        }

        private suspend fun Order.awaitStatus(status: Order.Status) {
            this.status.filter { it == status }.first()
        }

        private fun Order?.lastOrderWith(status: Order.Status): Order? {
            var i = this
            while (i != null) {
                if (i.status.value == status) {
                    break
                } else {
                    i = i.prevOrder
                }
            }
            return i
        }

        private fun Order?.getOrderWithClientOrderId(clientOrderId: Long): Order? {
            var i = this
            while (i != null) {
                if (i.clientOrderId == clientOrderId) break
                i = i.prevOrder
            }
            return i
        }

        private fun Order?.cleanUp() {
            var i = this
            var count = 0
            while (i != null) {
                if (i.status.value == Order.Status.Cancelled) count++
                if (count == 2) {
                    i.prevOrder = null
                    return
                }
                i = i.prevOrder
            }
        }

        private fun Order?.asSequence() = sequence<Order> {
            var i = this@asSequence
            while (i != null) {
                yield(i)
                i = i.prevOrder
            }
        }
    }
}
