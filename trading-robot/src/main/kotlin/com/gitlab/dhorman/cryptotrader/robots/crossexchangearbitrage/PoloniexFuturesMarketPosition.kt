package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheablePoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.service.poloniexfutures.PoloniexFuturesApi
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*

class PoloniexFuturesMarketPosition(
    private val cacheablePoloniexFuturesApi: CacheablePoloniexFuturesApi,
    override val market: String,
    override val quoteAmount: BigDecimal,
    override val side: PositionSide,
    private val contractSizeQty: BigDecimal,
    private val takerFee: BigDecimal,
    private val baseAssetPrecision: Int,
) : FuturesMarketPosition {
    private val _profit = MutableStateFlow(BigDecimal.ZERO)
    private val _state = MutableStateFlow(FuturesMarketPositionState.Considered)

    override val profit: StateFlow<BigDecimal> = _profit
    override val state: StateFlow<FuturesMarketPositionState> = _state

    override suspend fun open() {
        val orderSide = when (side) {
            PositionSide.Short -> PoloniexFuturesApi.OrderSide.Sell
            PositionSide.Long -> PoloniexFuturesApi.OrderSide.Buy
        }

        val id = UUID.randomUUID().toString()

        coroutineScope {
            val collectorReady = CompletableDeferred<Unit>()
            val collectorCompleted = CompletableDeferred<Unit>()

            launch {
                collectTrades(id, collectorReady, collectorCompleted)
            }

            collectorReady.await()

            cacheablePoloniexFuturesApi.api.placeOrder(
                PoloniexFuturesApi.PlaceOrderReq(
                    symbol = market,
                    clientOid = id,
                    type = PoloniexFuturesApi.PlaceOrderReq.Type.Market(
                        amount = PoloniexFuturesApi.PlaceOrderReq.Type.Amount.Currency(quoteAmount),
                    ),
                    openClose = PoloniexFuturesApi.PlaceOrderReq.OpenClose.Open(
                        side = orderSide,
                        leverage = BigDecimal.ONE,
                    ),
                )
            )

            collectorCompleted.await()

            _state.value = FuturesMarketPositionState.Opened
        }
    }

    override suspend fun close() {
        val id = UUID.randomUUID().toString()

        coroutineScope {
            val collectorReady = CompletableDeferred<Unit>()
            val collectorCompleted = CompletableDeferred<Unit>()

            launch {
                collectTrades(id, collectorReady, collectorCompleted)
            }

            collectorReady.await()

            cacheablePoloniexFuturesApi.api.placeOrder(
                PoloniexFuturesApi.PlaceOrderReq(
                    symbol = market,
                    clientOid = id,
                    type = PoloniexFuturesApi.PlaceOrderReq.Type.Market(
                        amount = PoloniexFuturesApi.PlaceOrderReq.Type.Amount.Currency(quoteAmount),
                    ),
                    openClose = PoloniexFuturesApi.PlaceOrderReq.OpenClose.Close,
                    reduceOnly = true,
                )
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
            cacheablePoloniexFuturesApi.api.privateMessagesStream.collect { event ->
                while (true) {
                    when (state) {
                        CollectTradesState.Connecting -> {
                            // TODO: Assuming that API must return first meta event quickly - subscribed = false
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
                                    is PoloniexFuturesApi.PrivateMessageEvent.OrderChange -> {
                                        if (event.payload.clientOid != id) return@collect

                                        val price = event.payload.matchPrice ?: return@collect
                                        val qty = (event.payload.matchSize ?: return@collect).toBigDecimal() * contractSizeQty

                                        val baseAmount = (price * qty).setScale(baseAssetPrecision, RoundingMode.DOWN)
                                        val fee = (baseAmount * takerFee).setScale(baseAssetPrecision, RoundingMode.UP)
                                        val baseAmountWithFee = baseAmount - fee

                                        _profit.value += when (event.payload.side) {
                                            PoloniexFuturesApi.OrderSide.Buy -> baseAmountWithFee.negate()
                                            PoloniexFuturesApi.OrderSide.Sell -> baseAmountWithFee
                                        }

                                        collectedQty += qty

                                        if (collectedQty.compareTo(quoteAmount) == 0) {
                                            state = CollectTradesState.Finished
                                        } else {
                                            return@collect
                                        }
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
