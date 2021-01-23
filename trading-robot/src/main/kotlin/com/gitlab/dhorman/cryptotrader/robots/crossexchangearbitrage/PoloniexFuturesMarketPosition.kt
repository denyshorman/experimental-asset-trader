package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexfutures.PoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheablePoloniexFuturesApi
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*

class PoloniexFuturesMarketPosition(
    private val cacheablePoloniexFuturesApi: CacheablePoloniexFuturesApi,
    private val market: String,
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

            logger.debug("Preparing to open order")

            collectorReady.await()

            logger.debug("Sending request to open order")

            val resp = cacheablePoloniexFuturesApi.api.placeOrder(
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

            logger.debug { "Order has been placed: $resp" }

            collectorCompleted.await()

            logger.debug("Order trades have been collected")

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

            logger.debug("Preparing to close order")

            collectorReady.await()

            logger.debug("Sending request to close order")

            val resp = cacheablePoloniexFuturesApi.api.placeOrder(
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
            cacheablePoloniexFuturesApi.privateMessagesStream.collect { event ->
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
                                    is PoloniexFuturesApi.PrivateMessageEvent.OrderChange -> {
                                        if (event.payload.clientOid != id) return@collect

                                        val price = event.payload.matchPrice ?: return@collect
                                        val qty = (event.payload.matchSize ?: return@collect).toBigDecimal() * contractSizeQty

                                        val baseAmount = (price * qty).setScale(baseAssetPrecision, RoundingMode.DOWN)
                                        val fee = (baseAmount * takerFee).setScale(baseAssetPrecision, RoundingMode.UP)

                                        _profit.value += when (event.payload.side) {
                                            PoloniexFuturesApi.OrderSide.Buy -> baseAmount.negate() - fee
                                            PoloniexFuturesApi.OrderSide.Sell -> baseAmount - fee
                                        }

                                        collectedQty += qty

                                        logger.debug { "Trade received: ${event.payload}" }

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

        other as PoloniexFuturesMarketPosition

        if (market != other.market) return false
        if (quoteAmount != other.quoteAmount) return false
        if (side != other.side) return false
        if (contractSizeQty != other.contractSizeQty) return false
        if (takerFee != other.takerFee) return false
        if (baseAssetPrecision != other.baseAssetPrecision) return false
        if (profit.value != other.profit.value) return false
        if (state.value != other.state.value) return false

        return true
    }

    override fun hashCode(): Int {
        var result = market.hashCode()
        result = 31 * result + quoteAmount.hashCode()
        result = 31 * result + side.hashCode()
        result = 31 * result + contractSizeQty.hashCode()
        result = 31 * result + takerFee.hashCode()
        result = 31 * result + baseAssetPrecision
        result = 31 * result + profit.value.hashCode()
        result = 31 * result + state.value.hashCode()
        return result
    }

    override fun toString(): String {
        return "PoloniexFuturesMarketPosition(market='$market', quoteAmount=$quoteAmount, side=$side, contractSizeQty=$contractSizeQty, takerFee=$takerFee, baseAssetPrecision=$baseAssetPrecision, profit=${profit.value}, state=${state.value})"
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
