package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.*
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.algo.simulateMarketOrderPnl
import com.gitlab.dhorman.cryptotrader.util.EventData
import io.vavr.collection.TreeMap
import io.vavr.kotlin.tuple
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertTrue

class CrossExchangeTraderTest {
    @Test
    fun crossExchangeTraderGeneralTest() {
        runBlocking {
            val leftMarketOrderBook = run {
                MutableStateFlow(
                    OrderBook(
                        asks = TreeMap.ofEntries(
                            tuple(BigDecimal("36559.0"), BigDecimal("1")),
                        ),
                        bids = TreeMap.ofEntries(
                            compareByDescending { it },
                            tuple(BigDecimal("36558.0"), BigDecimal("1")),
                        ),
                    )
                )
            }

            val rightMarketOrderBook = run {
                MutableStateFlow(
                    OrderBook(
                        asks = TreeMap.ofEntries(
                            tuple(BigDecimal("36521.0"), BigDecimal("1")),
                        ),
                        bids = TreeMap.ofEntries(
                            compareByDescending { it },
                            tuple(BigDecimal("36501.0"), BigDecimal("1")),
                        ),
                    )
                )
            }

            val leftMarket = object : FuturesMarket {
                override val generalInfo = run {
                    flowOf(
                        FuturesMarketGeneralInfo(
                            makerFee = BigDecimal("0.00075"),
                            takerFee = BigDecimal("0.00075"),
                            minQuoteAmount = BigDecimal("0.001"),
                            baseAssetPrecision = 8,
                            quotePrecision = 8,
                        )
                    )
                }

                override val orderBook: Flow<EventData<OrderBook>> = leftMarketOrderBook.map { EventData(it, true, null) }

                override suspend fun createMarketPosition(quoteAmount: BigDecimal, positionSide: PositionSide): FuturesMarketPosition {
                    return object : FuturesMarketPosition {
                        private val _profit = MutableStateFlow(BigDecimal.ZERO)
                        private val _state = MutableStateFlow(FuturesMarketPositionState.Considered)

                        override val state: StateFlow<FuturesMarketPositionState> = _state
                        override val side: PositionSide = positionSide
                        override val quoteAmount: BigDecimal = quoteAmount
                        override val profit: StateFlow<BigDecimal> = _profit

                        override suspend fun open() {
                            val info = generalInfo.first()
                            val book = orderBook.first().payload!!

                            val (baseAmount, _) = simulateMarketOrderPnl(
                                info.baseAssetPrecision,
                                quoteAmount,
                                info.takerFee,
                                if (side == PositionSide.Long) book.asks else book.bids,
                            )

                            _profit.value += if (side == PositionSide.Long) baseAmount.negate() else baseAmount
                            _state.value = FuturesMarketPositionState.Opened
                        }

                        override suspend fun close() {
                            val info = generalInfo.first()
                            val book = orderBook.first().payload!!

                            val (baseAmount, _) = simulateMarketOrderPnl(
                                info.baseAssetPrecision,
                                quoteAmount,
                                info.takerFee,
                                if (side == PositionSide.Long) book.bids else book.asks,
                            )

                            _profit.value += if (side == PositionSide.Long) baseAmount else baseAmount.negate()
                            _state.value = FuturesMarketPositionState.Closed
                        }

                        override fun equals(other: Any?): Boolean {
                            if (this === other) return true
                            if (javaClass != other?.javaClass) return false

                            other as PoloniexFuturesMarketPosition

                            if (quoteAmount != other.quoteAmount) return false
                            if (side != other.side) return false
                            if (profit.value != other.profit.value) return false
                            if (state.value != other.state.value) return false

                            return true
                        }

                        override fun hashCode(): Int {
                            var result = quoteAmount.hashCode()
                            result = 31 * result + side.hashCode()
                            result = 31 * result + profit.value.hashCode()
                            result = 31 * result + state.value.hashCode()
                            return result
                        }
                    }
                }
            }

            val rightMarket = object : FuturesMarket {
                override val generalInfo = run {
                    flowOf(
                        FuturesMarketGeneralInfo(
                            makerFee = BigDecimal("0.00075"),
                            takerFee = BigDecimal("0.00075"),
                            minQuoteAmount = BigDecimal("0.001"),
                            baseAssetPrecision = 8,
                            quotePrecision = 8,
                        )
                    )
                }

                override val orderBook: Flow<EventData<OrderBook>> = rightMarketOrderBook.map { EventData(it, true, null) }

                override suspend fun createMarketPosition(quoteAmount: BigDecimal, positionSide: PositionSide): FuturesMarketPosition {
                    return object : FuturesMarketPosition {
                        private val _profit = MutableStateFlow(BigDecimal.ZERO)
                        private val _state = MutableStateFlow(FuturesMarketPositionState.Considered)

                        override val state: StateFlow<FuturesMarketPositionState> = _state
                        override val side: PositionSide = positionSide
                        override val quoteAmount: BigDecimal = quoteAmount
                        override val profit: StateFlow<BigDecimal> = _profit

                        override suspend fun open() {
                            val info = generalInfo.first()
                            val book = orderBook.first().payload!!

                            val (baseAmount, _) = simulateMarketOrderPnl(
                                info.baseAssetPrecision,
                                quoteAmount,
                                info.takerFee,
                                if (side == PositionSide.Long) book.asks else book.bids,
                            )

                            _profit.value += if (side == PositionSide.Long) baseAmount.negate() else baseAmount
                            _state.value = FuturesMarketPositionState.Opened
                        }

                        override suspend fun close() {
                            val info = generalInfo.first()
                            val book = orderBook.first().payload!!

                            val (baseAmount, _) = simulateMarketOrderPnl(
                                info.baseAssetPrecision,
                                quoteAmount,
                                info.takerFee,
                                if (side == PositionSide.Long) book.bids else book.asks,
                            )

                            _profit.value += if (side == PositionSide.Long) baseAmount else baseAmount.negate()
                            _state.value = FuturesMarketPositionState.Closed
                        }

                        override fun equals(other: Any?): Boolean {
                            if (this === other) return true
                            if (javaClass != other?.javaClass) return false

                            other as PoloniexFuturesMarketPosition

                            if (quoteAmount != other.quoteAmount) return false
                            if (side != other.side) return false
                            if (profit.value != other.profit.value) return false
                            if (state.value != other.state.value) return false

                            return true
                        }

                        override fun hashCode(): Int {
                            var result = quoteAmount.hashCode()
                            result = 31 * result + side.hashCode()
                            result = 31 * result + profit.value.hashCode()
                            result = 31 * result + state.value.hashCode()
                            return result
                        }
                    }
                }
            }

            val trader = CrossExchangeTrader(
                leftMarket = leftMarket,
                rightMarket = rightMarket,
                maxOpenCost = BigDecimal("100"),
            )

            launch {
                // Open position
                println("Open position stage")
                trader.state.first { it == CrossExchangeTrader.State.OpenPosition }
                println("${trader.openStrategy.first()}")
                assertTrue(trader.currentProfit.first().compareTo(BigDecimal.ZERO) == 0)
                assertTrue(trader.onCloseProfit.first().compareTo(BigDecimal.ZERO) == 0)
                trader.openPositionThreshold.value = BigDecimal("0.06")

                // Close position
                trader.state.first { it == CrossExchangeTrader.State.ClosePosition }
                println("Close position stage")
                println("Current profit: ${trader.currentProfit.first()}")
                println("On close profit: ${trader.onCloseProfit.first()}")

                leftMarketOrderBook.value = OrderBook(
                    asks = TreeMap.ofEntries(
                        tuple(BigDecimal("36491.0"), BigDecimal("1")),
                    ),
                    bids = TreeMap.ofEntries(
                        compareByDescending { it },
                        tuple(BigDecimal("36490.0"), BigDecimal("1")),
                    ),
                )
                println("Order book changed")

                println("Current profit: ${trader.currentProfit.first()}")
                println("On close profit: ${trader.onCloseProfit.first()}")
                trader.closePositionThreshold.value = BigDecimal("0.07")
            }

            trader.trade()

            delay(1000)
            println("Final profit: ${trader.currentProfit.first()}")
        }
    }
}
