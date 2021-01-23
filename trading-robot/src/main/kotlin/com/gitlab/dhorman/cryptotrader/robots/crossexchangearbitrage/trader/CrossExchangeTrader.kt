package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.FuturesMarket
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.FuturesMarketPosition
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.PositionSide
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.algo.calculateFillAllMarketOrder
import com.gitlab.dhorman.cryptotrader.util.onlyPayload
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.time.Duration
import kotlin.time.minutes

class CrossExchangeTrader(
    private val leftMarket: FuturesMarket,
    private val rightMarket: FuturesMarket,
    private val maxOpenCost: BigDecimal,
) : AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("CrossExchangeTrader"))
    private val _state = MutableStateFlow(State.Init)
    private val _leftMarketPosition = MutableStateFlow<FuturesMarketPosition?>(null)
    private val _rightMarketPosition = MutableStateFlow<FuturesMarketPosition?>(null)
    private val _selectedStrategy = MutableStateFlow<Strategy?>(null)

    val state: StateFlow<State> = _state
    val openPositionThreshold = MutableStateFlow(Int.MAX_VALUE.toBigDecimal())
    val closePositionThreshold = MutableStateFlow(Int.MAX_VALUE.toBigDecimal())
    val leftMarketPosition: StateFlow<FuturesMarketPosition?> = _leftMarketPosition
    val rightMarketPosition: StateFlow<FuturesMarketPosition?> = _rightMarketPosition
    val selectedStrategy: StateFlow<Strategy?> = _selectedStrategy

    val openQuoteAmount = run {
        val leftOrderBookFlow = leftMarket.orderBook.onlyPayload()
        val rightOrderBookFlow = rightMarket.orderBook.onlyPayload()
        val orderBookFlow = leftOrderBookFlow.combine(rightOrderBookFlow) { l, r -> l to r }

        val leftMarketInfoFlow = leftMarket.generalInfo
        val rightMarketInfoFlow = rightMarket.generalInfo
        val marketInfoFlow = leftMarketInfoFlow.combine(rightMarketInfoFlow) { l, r -> l to r }

        val marketInfoAndOrderBookFlow = marketInfoFlow.combine(orderBookFlow) { m, o -> m to o }

        marketInfoAndOrderBookFlow
            .sample(500)
            .conflate()
            .map { (marketInfo, orderBooks) ->
                val (leftMarketInfo, rightMarketInfo) = marketInfo
                val (leftOrderBook, rightOrderBook) = orderBooks
                val leftPrice = leftOrderBook.asks.firstOrNull()?._1 ?: Int.MAX_VALUE.toBigDecimal()
                val rightPrice = rightOrderBook.asks.firstOrNull()?._1 ?: Int.MAX_VALUE.toBigDecimal()
                val leftQuoteAmount = maxOpenCost.divide(leftPrice, leftMarketInfo.quotePrecision, RoundingMode.DOWN)
                val rightQuoteAmount = maxOpenCost.divide(rightPrice, rightMarketInfo.quotePrecision, RoundingMode.DOWN)
                val leftMinQuoteAmount = leftMarketInfo.minQuoteAmount
                val rightMinQuoteAmount = rightMarketInfo.minQuoteAmount
                val leftContractsCount = leftQuoteAmount.divide(leftMinQuoteAmount, 0, RoundingMode.DOWN)
                val rightContractsCount = rightQuoteAmount.divide(rightMinQuoteAmount, 0, RoundingMode.DOWN)
                val leftAdjustedQuoteAmount = leftMinQuoteAmount * leftContractsCount
                val rightAdjustedQuoteAmount = rightMinQuoteAmount * rightContractsCount
                leftAdjustedQuoteAmount.min(rightAdjustedQuoteAmount)
            }
            .distinctUntilChanged { a, b -> a.compareTo(b) == 0 }
            .shareIn(scope, SharingStarted.WhileSubscribed(0, 0), 1)
    }

    val openStrategy = run {
        val leftOrderBookFlow = leftMarket.orderBook.onlyPayload()
        val rightOrderBookFlow = rightMarket.orderBook.onlyPayload()
        val orderBookFlow = leftOrderBookFlow.combine(rightOrderBookFlow) { l, r -> l to r }

        val leftMarketInfoFlow = leftMarket.generalInfo
        val rightMarketInfoFlow = rightMarket.generalInfo
        val marketInfoFlow = leftMarketInfoFlow.combine(rightMarketInfoFlow) { l, r -> l to r }

        val marketInfoOpenQuoteAmountFlow = marketInfoFlow.combine(openQuoteAmount) { i, q -> i to q }

        orderBookFlow.conflate().combine(marketInfoOpenQuoteAmountFlow) { oBooks, (marketInfo, quoteAmount) ->
            val (leftMarket, rightMarket) = marketInfo

            val (k0, k1) = calculateFillAllMarketOrder(
                oBooks.first,
                oBooks.second,
                leftMarket.takerFee,
                rightMarket.takerFee,
                leftMarket.baseAssetPrecision,
                rightMarket.baseAssetPrecision,
                quoteAmount,
            )

            StrategyCoeffs(quoteAmount, k0, k1)
        }.shareIn(scope, SharingStarted.WhileSubscribed(1.minutes, Duration.ZERO), 0)
    }

    val currentProfit = run {
        val leftPosProfitFlow = leftMarketPosition.filter { it != null }.flatMapLatest { it!!.profit }
        val rightPosProfitFlow = rightMarketPosition.filter { it != null }.flatMapLatest { it!!.profit }

        leftPosProfitFlow
            .combine(rightPosProfitFlow) { lPosProfit, rPosProfit -> lPosProfit + rPosProfit }
            .stateIn(scope, SharingStarted.Eagerly, BigDecimal.ZERO)
    }

    val onCloseProfit = run {
        state.flatMapLatest { state ->
            if (state == State.ClosePosition) {
                currentProfit.flatMapLatest { profit ->
                    openStrategy.transform { (_, k0, k1) ->
                        if (k0 == null || k1 == null) return@transform

                        val simulatedClosePnl = when (selectedStrategy.value!!) {
                            Strategy.LongShort -> k1
                            Strategy.ShortLong -> k0
                        }

                        emit(profit + simulatedClosePnl)
                    }
                }
            } else {
                currentProfit
            }
        }.shareIn(scope, SharingStarted.Lazily, 1)
    }

    override fun close() {
        scope.cancel()
    }

    suspend fun trade() {
        while (true) {
            when (state.value) {
                State.Init -> {
                    _state.value = State.OpenPosition
                }
                State.RebalanceWallet -> TODO()
                State.AllocateAmount -> TODO()
                State.OpenPosition -> {
                    val selectedStrategy = openPositionThreshold.combineTransform(openStrategy) { threshold, (quoteAmount, k0, k1) ->
                        if (k0 != null && k1 != null) {
                            if (k0 > threshold) {
                                emit(SelectedStrategy(Strategy.LongShort, quoteAmount, k0, k1))
                            } else if (k1 > threshold) {
                                emit(SelectedStrategy(Strategy.ShortLong, quoteAmount, k1, k0))
                            }
                        }
                    }.first()

                    val leftPositionSide: PositionSide
                    val rightPositionSide: PositionSide

                    when (selectedStrategy.strategy) {
                        Strategy.LongShort -> {
                            leftPositionSide = PositionSide.Long
                            rightPositionSide = PositionSide.Short
                        }
                        Strategy.ShortLong -> {
                            leftPositionSide = PositionSide.Short
                            rightPositionSide = PositionSide.Long
                        }
                    }

                    _selectedStrategy.value = selectedStrategy.strategy
                    _leftMarketPosition.value = leftMarket.createMarketPosition(selectedStrategy.quoteAmount, leftPositionSide)
                    _rightMarketPosition.value = rightMarket.createMarketPosition(selectedStrategy.quoteAmount, rightPositionSide)

                    coroutineScope {
                        launch {
                            leftMarketPosition.value!!.open()
                        }

                        launch {
                            rightMarketPosition.value!!.open()
                        }
                    }

                    _state.value = State.ClosePosition
                }
                State.ClosePosition -> {
                    closePositionThreshold.combineTransform(onCloseProfit) { threshold, profit ->
                        if (profit > threshold) {
                            emit(Unit)
                        }
                    }.first()

                    coroutineScope {
                        launch {
                            leftMarketPosition.value!!.close()
                        }

                        launch {
                            rightMarketPosition.value!!.close()
                        }
                    }

                    _state.value = State.Exit
                }
                State.Exit -> {
                    return
                }
            }
        }
    }


    enum class State {
        Init,
        RebalanceWallet,
        AllocateAmount,
        OpenPosition,
        ClosePosition,
        Exit,
    }

    enum class Strategy {
        LongShort,
        ShortLong,
    }

    data class SelectedStrategy(
        val strategy: Strategy,
        val quoteAmount: BigDecimal,
        val openProfit: BigDecimal,
        val closeProfit: BigDecimal,
    )

    data class StrategyCoeffs(
        val quoteAmount: BigDecimal,
        val k0: BigDecimal?,
        val k1: BigDecimal?,
    )
}
