package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.exchangesdk.binancefutures.BinanceFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheableBinanceFuturesApi
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*

class BinanceFuturesMarketPosition(
    private val binanceFuturesApi: CacheableBinanceFuturesApi,
    private val market: String,
    override val quoteAmount: BigDecimal,
    override val side: PositionSide,
    private val baseAssetPrecision: Int,
) : FuturesMarketPosition {
    private val _profit = MutableStateFlow(BigDecimal.ZERO)
    private val _state = MutableStateFlow(FuturesMarketPositionState.Considered)

    override val profit: StateFlow<BigDecimal> = _profit
    override val state: StateFlow<FuturesMarketPositionState> = _state

    override suspend fun open() {
        val id = UUID.randomUUID().toString()

        val orderSide = when (side) {
            PositionSide.Short -> BinanceFuturesApi.OrderSide.SELL
            PositionSide.Long -> BinanceFuturesApi.OrderSide.BUY
        }

        coroutineScope {
            val collectorReady = CompletableDeferred<Unit>()
            val collectorCompleted = CompletableDeferred<Unit>()

            launch {
                collectTrades(id, collectorReady, collectorCompleted)
            }

            logger.debug("Preparing to open order")

            collectorReady.await()

            logger.debug("Sending request to open order")

            val resp = binanceFuturesApi.api.placeNewOrder(
                symbol = market,
                newClientOrderId = id,
                side = orderSide,
                type = BinanceFuturesApi.OrderType.MARKET,
                quantity = quoteAmount,
                newOrderRespType = BinanceFuturesApi.ResponseType.RESULT,
            )

            logger.debug { "Order has been placed: $resp" }

            collectorCompleted.await()

            logger.debug("Order trades have been collected")

            _state.value = FuturesMarketPositionState.Opened
        }
    }

    override suspend fun close() {
        val id = UUID.randomUUID().toString()

        val orderSide = when (side) {
            PositionSide.Short -> BinanceFuturesApi.OrderSide.BUY
            PositionSide.Long -> BinanceFuturesApi.OrderSide.SELL
        }

        coroutineScope {
            val collectorReady = CompletableDeferred<Unit>()
            val collectorCompleted = CompletableDeferred<Unit>()

            launch {
                collectTrades(id, collectorReady, collectorCompleted)
            }

            logger.debug("Preparing to close order")

            collectorReady.await()

            logger.debug("Sending request to close order")

            val resp = binanceFuturesApi.api.placeNewOrder(
                symbol = market,
                newClientOrderId = id,
                side = orderSide,
                type = BinanceFuturesApi.OrderType.MARKET,
                quantity = quoteAmount,
                reduceOnly = true,
                newOrderRespType = BinanceFuturesApi.ResponseType.RESULT,
            )

            logger.debug { "Order has been placed: $resp" }

            collectorCompleted.await()

            logger.debug("Order trades have been collected")

            _state.value = FuturesMarketPositionState.Closed
        }
    }

    private suspend fun collectTrades(
        id: String,
        collectorReady: CompletableDeferred<Unit>,
        collectorCompleted: CompletableDeferred<Unit>,
    ) {
        var collectedQty = BigDecimal.ZERO
        var state = CollectTradesState.Connecting

        try {
            binanceFuturesApi.accountStream.collect { event ->
                while (true) {
                    when (state) {
                        CollectTradesState.Connecting -> {
                            if (event.subscribed) {
                                collectorReady.complete(Unit)
                                state = CollectTradesState.Collecting
                            }

                            return@collect
                        }
                        CollectTradesState.Collecting -> {
                            if (!event.subscribed || event.payload == null) {
                                state = CollectTradesState.Disconnected
                            } else {
                                when (event.payload) {
                                    is BinanceFuturesApi.AccountEvent.OrderTradeUpdateEvent -> {
                                        // TODO: Add additional filter to verify appropriate order type
                                        if (event.payload.order.clientOrderId != id) return@collect

                                        val price = event.payload.order.lastFilledPrice
                                        val qty = event.payload.order.orderLastFilledQuantity
                                        val fee = event.payload.order.commission ?: return@collect

                                        val baseAmount = (price * qty).setScale(baseAssetPrecision, RoundingMode.DOWN)

                                        _profit.value += when (event.payload.order.side) {
                                            BinanceFuturesApi.OrderSide.BUY -> baseAmount.negate() - fee
                                            BinanceFuturesApi.OrderSide.SELL -> baseAmount - fee
                                        }

                                        collectedQty += qty

                                        logger.debug { "Trade received: ${event.payload}" }

                                        if (collectedQty.compareTo(quoteAmount) == 0) {
                                            state = CollectTradesState.Finished
                                        } else {
                                            return@collect
                                        }
                                    }
                                    else -> {
                                        return@collect
                                    }
                                }
                            }
                        }
                        CollectTradesState.Finished -> {
                            logger.debug("All trades have been collected")
                            collectorCompleted.complete(Unit)
                            throw CancellationException()
                        }
                        CollectTradesState.Disconnected -> TODO("Reconnect and check trades")
                    }
                }
            }
        } catch (_: CancellationException) {
            // ignore
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BinanceFuturesMarketPosition

        if (market != other.market) return false
        if (quoteAmount != other.quoteAmount) return false
        if (side != other.side) return false
        if (baseAssetPrecision != other.baseAssetPrecision) return false
        if (profit.value != other.profit.value) return false
        if (state.value != other.state.value) return false

        return true
    }

    override fun hashCode(): Int {
        var result = market.hashCode()
        result = 31 * result + quoteAmount.hashCode()
        result = 31 * result + side.hashCode()
        result = 31 * result + baseAssetPrecision
        result = 31 * result + profit.value.hashCode()
        result = 31 * result + state.value.hashCode()
        return result
    }

    override fun toString(): String {
        return "BinanceFuturesMarketPosition(market='$market', quoteAmount=$quoteAmount, side=$side, baseAssetPrecision=$baseAssetPrecision, profit=${profit.value}, state=${state.value})"
    }

    private enum class CollectTradesState {
        Connecting,
        Collecting,
        Finished,
        Disconnected,
    }

    companion object {
        private val logger = KotlinLogging.logger {}
    }
}
