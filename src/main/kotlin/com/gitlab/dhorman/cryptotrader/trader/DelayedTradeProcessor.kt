package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.exception.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import com.gitlab.dhorman.cryptotrader.trader.algo.SplitTradeAlgo
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.core.PoloniexTradeAdjuster
import com.gitlab.dhorman.cryptotrader.trader.dao.TransactionsDao
import com.gitlab.dhorman.cryptotrader.util.returnLastIfNoValueWithinSpecifiedTime
import io.vavr.Tuple3
import io.vavr.collection.List
import io.vavr.kotlin.*
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
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.Comparator

class DelayedTradeProcessor(
    val market: Market,
    val orderType: OrderType,
    private val orderBook: Flow<OrderBookAbstract>,
    private val scope: CoroutineScope,
    splitAlgo: SplitTradeAlgo,
    private val poloniexApi: PoloniexApi,
    private val amountCalculator: AdjustedPoloniexBuySellAmountCalculator,
    private val data: DataStreams,
    private val transactionsDao: TransactionsDao
) {
    private val scheduler = TradeScheduler(orderType, splitAlgo, amountCalculator)
    private var tradeWorkerJob: Job? = null
    private val mutex = Mutex()

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
        var prevOrderId: Long? = null
        var currOrderId: Long? = null
        var latestTradeId: Long = -1
        var prevPrice: Price? = null
        var prevQuoteAmount: Amount? = null
        val orderCreateConfirmed = AtomicReference<CompletableDeferred<Unit>?>(null)
        val orderCancelConfirmed = AtomicReference<CompletableDeferred<OrderUpdateType>?>(null)
        val commonStateMutex = Mutex()

        // TODO: Rethink recover mechanism
        // recoverAfterPowerOff()

        val tradeMonitoringJob = launch(Job(), CoroutineStart.UNDISPATCHED) {
            poloniexApi.accountNotificationStream.collect { notifications ->
                var tradeList: LinkedList<BareTrade>? = null
                var orderCreateReceived: LimitOrderCreated? = null
                var orderCancelReceived: OrderUpdate? = null

                withContext(NonCancellable) {
                    commonStateMutex.withLock {
                        for (notification in notifications) {
                            when (notification) {
                                is TradeNotification -> run {
                                    if (currOrderId != null && notification.orderId == currOrderId || prevOrderId != null && notification.orderId == prevOrderId) {
                                        if (notification.tradeId > latestTradeId) {
                                            latestTradeId = notification.tradeId
                                        }

                                        if (tradeList == null) {
                                            tradeList = LinkedList()
                                        }

                                        tradeList!!.add(BareTrade(notification.amount, notification.price, notification.feeMultiplier))
                                    }
                                }
                                is LimitOrderCreated -> run {
                                    if (notification.orderId == currOrderId) {
                                        orderCreateReceived = notification
                                    }
                                }
                                is OrderUpdate -> run {
                                    if (notification.orderId == currOrderId && notification.newAmount.compareTo(BigDecimal.ZERO) == 0) {
                                        orderCancelReceived = notification
                                    }
                                }
                                else -> run {
                                    // ignore other events
                                }
                            }
                        }

                        if (tradeList != null && !tradeList!!.isEmpty()) {
                            scheduler.addTrades(tradeList!!)
                            thisJob?.cancel()
                        }

                        if (orderCreateReceived != null) orderCreateConfirmed.get()?.complete(Unit)
                        if (orderCancelReceived != null) orderCancelConfirmed.get()?.complete(orderCancelReceived!!.orderType)
                    }
                }
            }
        }

        try {
            var closeAllError: Throwable?

            while (isActive) {
                closeAllError = null

                logger.debug { "Start market maker trade process" }

                try {
                    coroutineScope {
                        launch(start = CoroutineStart.UNDISPATCHED) {
                            logger.debug { "Start connection monitoring" }

                            poloniexApi.connection.collect { connected ->
                                if (!connected) throw DisconnectedException
                            }
                        }

                        withContext(NonCancellable) {
                            commonStateMutex.withLock {
                                if (thisJob?.isCancelled == true) return@withContext

                                val placeOrderResult = placeOrder(scheduler.fromAmount)

                                logger.debug { "Order has been placed" }

                                currOrderId = placeOrderResult._1
                                prevPrice = placeOrderResult._2
                                prevQuoteAmount = placeOrderResult._3

                                orderCreateConfirmed.set(CompletableDeferred())
                                orderCancelConfirmed.set(CompletableDeferred())
                            }
                        }

                        orderCreateConfirmed.get()?.await()

                        var prevFromAmount = scheduler.fromAmount
                        val timeout = Duration.ofSeconds(4)
                        val timeoutMillis = timeout.toMillis()
                        var bookUpdateTime = System.currentTimeMillis()
                        var bookChangeCounter = 0
                        var prevBook: OrderBookAbstract? = null

                        logger.debug { "Start market maker movement process" }

                        orderBook.returnLastIfNoValueWithinSpecifiedTime(timeout).conflate().collect { book ->
                            try {
                                withContext(NonCancellable) {
                                    commonStateMutex.withLock {
                                        if (thisJob?.isCancelled == true) return@withContext

                                        bookChangeCounter = if (bookChangeCounter >= 10) 0 else bookChangeCounter + 1
                                        val currFromAmount = scheduler.fromAmount
                                        val forceMove = prevFromAmount.compareTo(currFromAmount) != 0
                                        val now = System.currentTimeMillis()
                                        val fixPriceGaps = now - bookUpdateTime >= timeoutMillis || bookChangeCounter == 0
                                        bookUpdateTime = now
                                        prevFromAmount = currFromAmount
                                        prevBook = book

                                        val (orderId, price, quoteAmount) = moveOrder(
                                            book,
                                            currOrderId!!,
                                            prevPrice!!,
                                            prevQuoteAmount!!,
                                            currFromAmount,
                                            forceMove,
                                            fixPriceGaps
                                        ) ?: return@withContext

                                        logger.debug { "Moved order (q = $quoteAmount, p = $price)" }

                                        prevOrderId = currOrderId
                                        currOrderId = orderId
                                        prevPrice = price
                                        prevQuoteAmount = quoteAmount

                                        orderCreateConfirmed.set(CompletableDeferred())
                                        orderCancelConfirmed.set(CompletableDeferred())
                                    }
                                }
                            } catch (e: UnableToPlacePostOnlyOrderException) {
                                logger.debug {
                                    val asks = prevBook?.asks?.take(3)?.joinToString(separator = ";") { "(${it._1},${it._2})" }
                                    val bids = prevBook?.bids?.take(3)?.joinToString(separator = ";") { "(${it._1},${it._2})" }
                                    "${e.message} ; book = (asks = $asks ; bids = $bids) ; currentOrderId = $currOrderId; previousPrice = $prevPrice; " +
                                        "previousQuoteAmount = $prevQuoteAmount; currFromAmount = $prevFromAmount"
                                }
                                return@collect
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
                                        logger.debug("${e.message}; fromAmount: ${scheduler.fromAmount};")
                                        delay(60000)
                                        throw e
                                    }
                                    else -> throw e
                                }
                            }

                            orderCreateConfirmed.get()?.await()
                        }
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: DisconnectedException) {
                    logger.debug { "Disconnected from Poloniex" }

                    withContext(NonCancellable) {
                        poloniexApi.connection.filter { it }.first()
                        logger.debug { "Connected to Poloniex" }
                    }
                } catch (e: Throwable) {
                    logger.debug("${e.message}; fromAmount: ${scheduler.fromAmount};")
                    closeAllError = e
                } finally {
                    withContext(NonCancellable) {
                        try {
                            if (currOrderId == null) {
                                logger.debug { "Don't have any opened orders" }
                                return@withContext
                            }

                            val cancelConfirmedDef = orderCancelConfirmed.get()
                            val cancelConfirmed = cancelConfirmedDef?.isCompleted ?: true
                            if (!cancelConfirmed) cancelOrder(currOrderId!!)
                            val cancelConfirmTimeout = withTimeoutOrNull(Duration.ofMinutes(1).toMillis()) {
                                logger.debug { "Waiting for order cancel confirmation..." }
                                cancelConfirmedDef?.await()
                                logger.debug { "Order cancel confirmation event has been received." }
                            }

                            if (cancelConfirmTimeout == null) {
                                logger.warn { "Cancel event has not been received within specified timeout" }
                            }

                            val processedMissedTrades = processMissedTrades(currOrderId!!, latestTradeId)

                            if (processedMissedTrades) {
                                thisJob?.cancel()
                            }

                            currOrderId = null
                            prevOrderId = null
                        } finally {
                            if (closeAllError != null) {
                                scheduler.unregisterAll(closeAllError)
                                thisJob?.cancel()
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

    private suspend fun placeOrder(fromAmountValue: BigDecimal): Tuple3<Long, Price, Amount> {
        while (true) {
            try {
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

                if (quoteAmount.compareTo(BigDecimal.ZERO) == 0) throw AmountIsZeroException

                val result = poloniexApi.placeLimitOrder(market, orderType, price, quoteAmount, BuyOrderType.PostOnly)

                return tuple(result.orderId, price, quoteAmount)
            } catch (e: CancellationException) {
                throw e
            } catch (e: UnableToPlacePostOnlyOrderException) {
                logger.debug(e.originalMsg)
                delay(100)
                continue
            } catch (e: TransactionFailedException) {
                logger.debug(e.originalMsg)
                delay(500)
                continue
            } catch (e: NotEnoughCryptoException) {
                throw e
            } catch (e: AmountMustBeAtLeastException) {
                throw e
            } catch (e: TotalMustBeAtLeastException) {
                throw e
            } catch (e: RateMustBeLessThanException) {
                throw e
            } catch (e: MarketDisabledException) {
                throw e
            } catch (e: OrderBookEmptyException) {
                logger.warn(e.message)
                delay(1000)
                continue
            } catch (e: MaxOrdersExceededException) {
                logger.warn(e.originalMsg)
                delay(1500)
                continue
            } catch (e: UnknownHostException) {
                delay(2000)
                continue
            } catch (e: IOException) {
                delay(2000)
                continue
            } catch (e: ConnectException) {
                delay(2000)
                continue
            } catch (e: SocketException) {
                delay(2000)
                continue
            } catch (e: Throwable) {
                delay(2000)
                if (logger.isDebugEnabled) logger.error(e.message)
                continue
            }
        }
    }

    private suspend fun moveOrder(
        book: OrderBookAbstract,
        lastOrderId: Long,
        myPrice: Price,
        myQuoteAmount: Amount,
        fromAmountValue: Amount,
        forceMove: Boolean,
        fixPriceGaps: Boolean = false
    ): Tuple3<Long, Price, Amount>? {
        while (true) {
            try {
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
                    val myPositionInBook = priceComparator.compare(bookPrice1, myPrice)

                    if (myPositionInBook == 1 || myPositionInBook == 0 && myQuoteAmount.compareTo(bookQuoteAmount1) != 0) {
                        // I am on second position
                        var price = moveToOnePoint(bookPrice1)
                        val ask = secondaryBook.headOption().map { it._1 }.orNull
                        if (ask != null && ask.compareTo(price) == 0) price = bookPrice1
                        price
                    } else {
                        // I am on first position
                        if (fixPriceGaps) {
                            val secondPrice = primaryBook.drop(1).headOption().map { moveToOnePoint(it._1) }.orNull
                            if (secondPrice != null && priceComparator.compare(myPrice, secondPrice) == 1) secondPrice else null
                        } else {
                            null
                        }
                    }
                } ?: if (forceMove) myPrice else return null

                val quoteAmount = when (orderType) {
                    OrderType.Buy -> amountCalculator.quoteAmount(fromAmountValue, newPrice, BigDecimal.ONE)
                    OrderType.Sell -> fromAmountValue
                }

                if (quoteAmount.compareTo(BigDecimal.ZERO) == 0) throw AmountIsZeroException

                val moveOrderResult = poloniexApi.moveOrder(
                    lastOrderId,
                    newPrice,
                    quoteAmount,
                    BuyOrderType.PostOnly
                )

                return tuple(moveOrderResult.orderId, newPrice, quoteAmount)
            } catch (e: CancellationException) {
                throw e
            } catch (e: OrderCompletedOrNotExistException) {
                throw e
            } catch (e: UnableToPlacePostOnlyOrderException) {
                throw e
            } catch (e: TransactionFailedException) {
                logger.debug(e.originalMsg)
                delay(500)
            } catch (e: InvalidOrderNumberException) {
                throw e
            } catch (e: NotEnoughCryptoException) {
                throw e
            } catch (e: AmountMustBeAtLeastException) {
                throw e
            } catch (e: TotalMustBeAtLeastException) {
                throw e
            } catch (e: RateMustBeLessThanException) {
                throw e
            } catch (e: OrderBookEmptyException) {
                logger.warn(e.message)
                delay(1000)
            } catch (e: MaxOrdersExceededException) {
                logger.warn(e.originalMsg)
                delay(1500)
            } catch (e: UnknownHostException) {
                delay(2000)
            } catch (e: IOException) {
                delay(2000)
            } catch (e: ConnectException) {
                delay(2000)
            } catch (e: SocketException) {
                delay(2000)
            } catch (e: Throwable) {
                delay(2000)
                if (logger.isDebugEnabled) logger.error(e.message)
            }
        }
    }

    private suspend fun cancelOrder(orderId: Long) {
        while (true) {
            try {
                poloniexApi.cancelOrder(orderId)
                break
            } catch (e: OrderCompletedOrNotExistException) {
                logger.debug(e.message)
                cancelUncaughtOrders()
                break
            } catch (e: UnknownHostException) {
                delay(2000)
            } catch (e: IOException) {
                delay(2000)
            } catch (e: ConnectException) {
                delay(2000)
            } catch (e: SocketException) {
                delay(2000)
            } catch (e: Throwable) {
                logger.error(e.message)
                delay(2000)
            }
        }

        logger.debug { "Order $orderId has been cancelled" }
    }

    private suspend fun cancelUncaughtOrders() {
        logger.debug("Trying to find and cancel uncaught orders")

        coroutineScope {
            forceGetOpenOrders(market)
                .asSequence()
                .filter { it.type == orderType }
                .map { async { cancelOrder(it.orderId) } }
                .toList()
                .forEach { it.await() }
        }

        logger.debug("All uncaught orders have been processed")
    }

    private suspend fun forceGetOpenOrders(market: Market): List<OpenOrder> {
        while (true) {
            try {
                return poloniexApi.openOrders(market)
            } catch (e: UnknownHostException) {
                delay(2000)
            } catch (e: IOException) {
                delay(2000)
            } catch (e: ConnectException) {
                delay(2000)
            } catch (e: SocketException) {
                delay(2000)
            } catch (e: Throwable) {
                logger.error(e.message)
                delay(2000)
            }
        }
    }

    private suspend fun recoverAfterPowerOff() {
        logger.debug { "Trying to recover after power off..." }
        val (orderId, tradeId) = transactionsDao.getLatestOrderAndTradeId(market, orderType) ?: run {
            logger.debug { "Recover after power off is not required." }
            return
        }
        val orderIsOpen = data.openOrders.first().getOrNull(orderId) != null
        if (orderIsOpen) cancelOrder(orderId)
        processMissedTrades(orderId, tradeId)
    }

    private suspend fun processMissedTrades(orderId: Long, latestTradeId: Long): Boolean {
        while (true) {
            try {
                logger.debug { "Trying to process missed trades..." }

                val tradeList = poloniexApi.orderTrades(orderId).asSequence()
                    .filter { it.tradeId > latestTradeId }
                    .map { trade -> BareTrade(trade.amount, trade.price, trade.feeMultiplier) }
                    .toList()

                return if (tradeList.isNotEmpty()) {
                    logger.debug { "Processing missed trades $tradeList..." }
                    scheduler.addTrades(tradeList)
                    true
                } else {
                    false
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (logger.isDebugEnabled) logger.debug(e.message)
                delay(1000)
            }
        }
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }
}
