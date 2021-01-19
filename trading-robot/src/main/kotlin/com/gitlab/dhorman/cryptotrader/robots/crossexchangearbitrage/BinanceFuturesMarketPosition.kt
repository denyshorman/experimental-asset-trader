package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.exchangesdk.binancefutures.BinanceFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheableBinanceFuturesApi
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*

class BinanceFuturesMarketPosition(
    private val binanceFuturesApi: CacheableBinanceFuturesApi,
    override val market: String,
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

            collectorReady.await()

            binanceFuturesApi.api.placeNewOrder(
                symbol = market,
                newClientOrderId = id,
                side = orderSide,
                type = BinanceFuturesApi.OrderType.MARKET,
                quantity = quoteAmount,
                newOrderRespType = BinanceFuturesApi.ResponseType.RESULT,
            )

            collectorCompleted.await()

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

            collectorReady.await()

            binanceFuturesApi.api.placeNewOrder(
                symbol = market,
                newClientOrderId = id,
                side = orderSide,
                type = BinanceFuturesApi.OrderType.MARKET,
                quantity = quoteAmount,
                reduceOnly = true,
                newOrderRespType = BinanceFuturesApi.ResponseType.RESULT,
            )

            collectorCompleted.await()

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
                                        val baseAmountWithFee = baseAmount - fee

                                        _profit.value += when (event.payload.order.side) {
                                            BinanceFuturesApi.OrderSide.BUY -> baseAmountWithFee.negate()
                                            BinanceFuturesApi.OrderSide.SELL -> baseAmountWithFee
                                        }

                                        collectedQty += qty

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

    private enum class CollectTradesState {
        Connecting,
        Collecting,
        Finished,
        Disconnected,
    }
}
