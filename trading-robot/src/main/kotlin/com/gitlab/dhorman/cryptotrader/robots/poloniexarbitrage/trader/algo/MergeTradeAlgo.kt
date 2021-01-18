package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.algo

import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.*
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Amount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderType
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.core.AdjustedBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.core.TradeAdjuster
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.model.TranIntentMarketPartiallyCompleted
import io.vavr.Tuple2
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.tuple
import java.math.BigDecimal
import java.math.RoundingMode

class MergeTradeAlgo(
    private val amountCalculator: AdjustedBuySellAmountCalculator,
    private val tradeAdjuster: TradeAdjuster,
    private val tranIntentMarketExtensions: TranIntentMarketExtensions
) {
    fun mergeMarkets(currentMarkets: Array<TranIntentMarket>, unfilledMarkets: List<Tuple2<Amount, Amount>>?): Array<TranIntentMarket> {
        if (unfilledMarkets == null || unfilledMarkets.length() == 0) return currentMarkets

        var newMarkets = currentMarkets

        for (amounts in unfilledMarkets) {
            newMarkets = mergeMarkets(newMarkets, amounts._1, amounts._2)
        }

        return newMarkets
    }

    fun mergeMarkets(currentMarkets: Array<TranIntentMarket>, initCurrencyAmount: Amount, currentCurrencyAmount: Amount): Array<TranIntentMarket> {
        var updatedMarkets = currentMarkets
        val currMarketIdx = tranIntentMarketExtensions.partiallyCompletedMarketIndex(currentMarkets)!!
        val prevMarketIdx = currMarketIdx - 1

        val oldCurrentMarket = updatedMarkets[currMarketIdx] as TranIntentMarketPartiallyCompleted
        var targetAmount = if (prevMarketIdx >= 0) {
            tranIntentMarketExtensions.targetAmount(updatedMarkets[prevMarketIdx] as TranIntentMarketCompleted) + currentCurrencyAmount
        } else {
            oldCurrentMarket.fromAmount + initCurrencyAmount
        }

        // 1. Update current market.
        val newCurrentMarket = TranIntentMarketPartiallyCompleted(
            oldCurrentMarket.market,
            oldCurrentMarket.orderSpeed,
            oldCurrentMarket.fromCurrencyType,
            targetAmount
        )
        updatedMarkets = updatedMarkets.update(currMarketIdx, newCurrentMarket)

        // 2. Update another markets.

        for (i in prevMarketIdx downTo 0) {
            val market = updatedMarkets[i] as TranIntentMarketCompleted
            val targetAmountDelta = targetAmount - tranIntentMarketExtensions.targetAmount(market)

            if (targetAmountDelta.compareTo(BigDecimal.ZERO) == 0) break

            val trade = when (market.orderType) {
                OrderType.Buy -> run {
                    val price = market.trades.asSequence().filter { !tradeAdjuster.isAdjustmentTrade(it) }.map { it.price }.maxOrNull() ?: BigDecimal.ONE
                    val fee = market.trades.asSequence().filter { !tradeAdjuster.isAdjustmentTrade(it) }.map { it.feeMultiplier }.maxOrNull() ?: BigDecimal.ONE
                    val quoteAmount = targetAmountDelta.divide(fee, 8, RoundingMode.DOWN)
                    BareTrade(quoteAmount, price, fee)
                }
                OrderType.Sell -> run {
                    val price = market.trades.asSequence().filter { !tradeAdjuster.isAdjustmentTrade(it) }.map { it.price }.minOrNull() ?: BigDecimal.ONE
                    val fee = market.trades.asSequence().filter { !tradeAdjuster.isAdjustmentTrade(it) }.map { it.feeMultiplier }.minOrNull() ?: BigDecimal.ONE
                    val quoteAmount = targetAmountDelta.divide(price, 8, RoundingMode.UP).divide(fee, 8, RoundingMode.UP)
                    BareTrade(quoteAmount, price, fee)
                }
            }

            var newTrades = market.trades.append(trade)
            val targetAmountNew = tranIntentMarketExtensions.targetAmount(newTrades, market.orderType)
            val targetAmountNewDelta = targetAmount - targetAmountNew
            if (targetAmountNewDelta.compareTo(BigDecimal.ZERO) != 0) {
                val adjTrade = tradeAdjuster.adjustTargetAmount(targetAmountNewDelta, market.orderType)
                newTrades = newTrades.append(adjTrade)
            }
            val newMarket = TranIntentMarketCompleted(market.market, market.orderSpeed, market.fromCurrencyType, newTrades)
            updatedMarkets = updatedMarkets.update(i, newMarket)
            targetAmount = tranIntentMarketExtensions.fromAmount(newMarket)
        }

        // 3. Add an amount to trades of init market.

        if (prevMarketIdx >= 0 && initCurrencyAmount.compareTo(BigDecimal.ZERO) != 0) {
            val initMarket = updatedMarkets[0] as TranIntentMarketCompleted

            val fromAmountAllInitial = tranIntentMarketExtensions.fromAmount(currentMarkets[0] as TranIntentMarketCompleted)
            val fromAmountCalculated = tranIntentMarketExtensions.fromAmount(initMarket)
            val deltaAmount = fromAmountAllInitial + initCurrencyAmount - fromAmountCalculated

            if (deltaAmount.compareTo(BigDecimal.ZERO) != 0) {
                val firstTradeIdx = initMarket.trades.asSequence()
                    .mapIndexed { i, trade -> tuple(i, trade) }
                    .filter { !tradeAdjuster.isAdjustmentTrade(it._2) }
                    .map { (i, trade) ->
                        val fromAmount = amountCalculator.fromAmount(initMarket.orderType, trade)
                        tuple(i, trade, fromAmount + deltaAmount)
                    }
                    .filter { it._3 > BigDecimal.ZERO }
                    .sortedByDescending { it._3 }
                    .map { it._1 }
                    .firstOrNull()

                if (firstTradeIdx != null) {
                    val trade = initMarket.trades[firstTradeIdx]
                    val newTradesList = when (initMarket.orderType) {
                        OrderType.Buy -> {
                            val fromAmount = amountCalculator.fromAmount(initMarket.orderType, trade)
                            val newFromAmount = fromAmount + deltaAmount
                            val newPrice = newFromAmount.divide(trade.quoteAmount, 8, RoundingMode.DOWN)
                            val newTrade = BareTrade(trade.quoteAmount, newPrice, trade.feeMultiplier)
                            val calcNewFromAmount = amountCalculator.fromAmount(initMarket.orderType, newTrade)
                            val newFromAmountDelta = newFromAmount - calcNewFromAmount
                            if (newFromAmountDelta.compareTo(BigDecimal.ZERO) != 0) {
                                val adjTrade = tradeAdjuster.adjustFromAmount(newFromAmountDelta)
                                listOf(newTrade, adjTrade)
                            } else {
                                listOf(newTrade)
                            }
                        }
                        OrderType.Sell -> {
                            val tradeFromAmount = amountCalculator.fromAmount(initMarket.orderType, trade)
                            val tradeTargetAmount = amountCalculator.targetAmount(initMarket.orderType, trade)
                            val newFromAmount = tradeFromAmount + deltaAmount
                            val newPrice = tradeTargetAmount.divide(newFromAmount, 8, RoundingMode.DOWN).divide(trade.feeMultiplier, 8, RoundingMode.DOWN)
                            val newTrade = BareTrade(newFromAmount, newPrice, trade.feeMultiplier)
                            val calcNewTargetAmount = amountCalculator.targetAmount(initMarket.orderType, newTrade)
                            val newTargetAmountDelta = tradeTargetAmount - calcNewTargetAmount
                            if (newTargetAmountDelta.compareTo(BigDecimal.ZERO) != 0) {
                                val adjTrade = tradeAdjuster.adjustTargetAmount(newTargetAmountDelta, initMarket.orderType)
                                listOf(newTrade, adjTrade)
                            } else {
                                listOf(newTrade)
                            }
                        }
                    }
                    val newTrades = initMarket.trades.removeAt(firstTradeIdx).appendAll(newTradesList)
                    val newMarket = TranIntentMarketCompleted(initMarket.market, initMarket.orderSpeed, initMarket.fromCurrencyType, newTrades)
                    updatedMarkets = updatedMarkets.update(0, newMarket)
                } else {
                    val newTrade = tradeAdjuster.adjustFromAmount(deltaAmount)
                    val newTrades = initMarket.trades.append(newTrade)
                    val newMarket = TranIntentMarketCompleted(initMarket.market, initMarket.orderSpeed, initMarket.fromCurrencyType, newTrades)
                    updatedMarkets = updatedMarkets.update(0, newMarket)
                }
            }
        }

        return updatedMarkets
    }
}
