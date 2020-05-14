package com.gitlab.dhorman.cryptotrader.trader.algo

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.core.TradeAdjuster
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import io.vavr.Tuple2
import io.vavr.collection.Array
import io.vavr.collection.List
import io.vavr.kotlin.*
import java.math.BigDecimal
import java.math.RoundingMode

class SplitTradeAlgo(
    private val amountCalculator: AdjustedBuySellAmountCalculator,
    private val tradeAdjuster: TradeAdjuster,
    private val tranIntentMarketExtensions: TranIntentMarketExtensions
) {
    private fun splitAdjustedTrade(trade: BareTrade, fromOrToAmount: Amount): Tuple2<List<BareTrade>, List<BareTrade>> {
        val q0 = fromOrToAmount
        val q1 = trade.quoteAmount - q0

        if (q1 < BigDecimal.ZERO) throw RuntimeException("Adjustment trade $trade is not supported")

        val commitTrade = BareTrade(q0, trade.price, trade.feeMultiplier)
        val updateTrade = BareTrade(q1, trade.price, trade.feeMultiplier)

        return tuple(list(commitTrade), list(updateTrade))
    }

    private fun BigDecimal.adjustFrom(): BareTrade? {
        if (this.compareTo(BigDecimal.ZERO) == 0) return null
        return tradeAdjuster.adjustFromAmount(this)
    }

    private fun BigDecimal.adjustTarget(orderType: OrderType): BareTrade? {
        if (this.compareTo(BigDecimal.ZERO) == 0) return null
        return tradeAdjuster.adjustTargetAmount(this, orderType)
    }

    private fun BigDecimal.adjust(amountType: AmountType, orderType: OrderType): BareTrade? {
        return when (amountType) {
            AmountType.From -> this.adjustFrom()
            AmountType.Target -> this.adjustTarget(orderType)
        }
    }

    private fun adjust(amountType: AmountType, orderType: OrderType, delta: BigDecimal, update: BigDecimal, commit: BigDecimal): Tuple2<BareTrade?, BareTrade?> {
        if (delta.compareTo(BigDecimal.ZERO) == 0) return tuple(null, null)
        if (delta > BigDecimal.ZERO) return tuple(null, delta.adjust(amountType, orderType))
        if (commit + delta >= BigDecimal.ZERO) return tuple(null, delta.adjust(amountType, orderType))
        if (update + delta >= BigDecimal.ZERO) return tuple(delta.adjust(amountType, orderType), null)
        if (commit + update + delta < BigDecimal.ZERO) throw RuntimeException("Resulting amount can't be negative")
        return tuple(update.adjust(amountType, orderType), (update + delta).adjust(amountType, orderType))
    }

    fun splitTrade(amountType: AmountType, orderType: OrderType, amount: Amount, trade: BareTrade): Tuple2<List<BareTrade>, List<BareTrade>> {
        if (tradeAdjuster.isAdjustmentTrade(trade)) return splitAdjustedTrade(trade, amount)

        val tradeWithdraw = amountCalculator.fromAmount(orderType, trade)
        val tradeDeposit = amountCalculator.targetAmount(orderType, trade)

        val (commitQuoteAmount, updateQuoteAmount) = when (amountType) {
            AmountType.From -> when (orderType) {
                OrderType.Buy -> run {
                    val commitQuoteAmount = amount.divide(trade.price, 8, RoundingMode.DOWN)
                    val updateQuoteAmount = tradeWithdraw.divide(trade.price, 8, RoundingMode.DOWN) - commitQuoteAmount
                    tuple(commitQuoteAmount, updateQuoteAmount)
                }
                OrderType.Sell -> tuple(amount, tradeWithdraw - amount)
            }
            AmountType.Target -> when (orderType) {
                OrderType.Buy -> run {
                    val commitQuoteAmount = amount.divide(trade.feeMultiplier, 8, RoundingMode.DOWN)
                    val updateQuoteAmount = tradeDeposit.divide(trade.feeMultiplier, 8, RoundingMode.DOWN) - commitQuoteAmount
                    tuple(commitQuoteAmount, updateQuoteAmount)
                }
                OrderType.Sell -> run {
                    val pf = (trade.price * trade.feeMultiplier).setScale(8, RoundingMode.DOWN)
                    val commitQuoteAmount = amount.divide(pf, 8, RoundingMode.DOWN)
                    val updateQuoteAmount = tradeDeposit.divide(pf, 8, RoundingMode.DOWN) - commitQuoteAmount
                    tuple(commitQuoteAmount, updateQuoteAmount)
                }
            }
        }

        val updateTrade = BareTrade(updateQuoteAmount, trade.price, trade.feeMultiplier)
        val commitTrade = BareTrade(commitQuoteAmount, trade.price, trade.feeMultiplier)

        val commitAmount = amountCalculator.amount(!amountType, orderType, commitTrade)
        val updateAmount = amountCalculator.amount(!amountType, orderType, updateTrade)

        val delta = run {
            val tradeAmount = when (amountType) {
                AmountType.From -> tradeDeposit
                AmountType.Target -> tradeWithdraw
            }

            tradeAmount - commitAmount - updateAmount
        }

        val (updateTradeWithdrawAdj, updateTradeDepositAdj, commitTradeWithdrawAdj, commitTradeDepositAdj) = when (amountType) {
            AmountType.From -> run {
                val (updateTradeWithdrawAdj, commitTradeWithdrawAdj) = when (orderType) {
                    OrderType.Buy -> run {
                        val withdrawDeltaCommit = amount - amountCalculator.fromAmount(orderType, commitTrade)
                        val withdrawDeltaUpdate = tradeWithdraw - amount - amountCalculator.fromAmount(orderType, updateTrade)
                        val commitTradeWithdrawAdj = withdrawDeltaCommit.adjust(AmountType.From, orderType)
                        val updateTradeWithdrawAdj = withdrawDeltaUpdate.adjust(AmountType.From, orderType)
                        tuple(updateTradeWithdrawAdj, commitTradeWithdrawAdj)
                    }
                    OrderType.Sell -> tuple(null, null)
                }
                val (updateTradeDepositAdj, commitTradeDepositAdj) = adjust(AmountType.Target, orderType, delta, updateAmount, commitAmount)
                tuple(updateTradeWithdrawAdj, updateTradeDepositAdj, commitTradeWithdrawAdj, commitTradeDepositAdj)
            }
            AmountType.Target -> run {
                val commitDepositDelta = amount - amountCalculator.targetAmount(orderType, commitTrade)
                val updateDepositDelta = tradeDeposit - amount - amountCalculator.targetAmount(orderType, updateTrade)
                val (updateTradeWithdrawAdj, commitTradeWithdrawAdj) = adjust(AmountType.From, orderType, delta, updateAmount, commitAmount)
                val commitTradeDepositAdj = commitDepositDelta.adjust(AmountType.Target, orderType)
                val updateTradeDepositAdj = updateDepositDelta.adjust(AmountType.Target, orderType)
                tuple(updateTradeWithdrawAdj, updateTradeDepositAdj, commitTradeWithdrawAdj, commitTradeDepositAdj)
            }
        }

        val updateTrades = listOfNotNull(updateTrade, updateTradeWithdrawAdj, updateTradeDepositAdj).toVavrList()
        val commitTrades = listOfNotNull(commitTrade, commitTradeWithdrawAdj, commitTradeDepositAdj).toVavrList()

        return tuple(updateTrades, commitTrades)
    }

    fun splitMarkets(markets: Array<TranIntentMarket>, currentMarketIdx: Int, trades: Array<BareTrade>): Tuple2<Array<TranIntentMarket>, Array<TranIntentMarket>> {
        val selectedMarket = markets[currentMarketIdx] as TranIntentMarketPartiallyCompleted

        var updatedMarkets = markets
        var committedMarkets = markets

        // Commit current market and prepare next

        val marketCompleted = TranIntentMarketCompleted(
            selectedMarket.market,
            selectedMarket.orderSpeed,
            selectedMarket.fromCurrencyType,
            trades
        )

        committedMarkets = committedMarkets.update(currentMarketIdx, marketCompleted)

        val nextMarketIdx = currentMarketIdx + 1

        if (nextMarketIdx < markets.length()) {
            val nextMarket = markets[nextMarketIdx]
            val nextMarketInit = TranIntentMarketPartiallyCompleted(
                nextMarket.market,
                nextMarket.orderSpeed,
                nextMarket.fromCurrencyType,
                tranIntentMarketExtensions.targetAmount(marketCompleted)
            )
            committedMarkets = committedMarkets.update(nextMarketIdx, nextMarketInit)
        }

        // Update current market

        val updatedMarket = TranIntentMarketPartiallyCompleted(
            selectedMarket.market,
            selectedMarket.orderSpeed,
            selectedMarket.fromCurrencyType,
            selectedMarket.fromAmount - tranIntentMarketExtensions.fromAmount(marketCompleted)
        )

        updatedMarkets = updatedMarkets.update(currentMarketIdx, updatedMarket)


        // Split trades of previous markets
        var i = currentMarketIdx - 1

        while (i >= 0) {
            val m = markets[i] as TranIntentMarketCompleted

            val updatedTrades = mutableListOf<BareTrade>()
            val committedTrades = mutableListOf<BareTrade>()

            var targetAmount = tranIntentMarketExtensions.fromAmount(committedMarkets[i + 1] as TranIntentMarketCompleted)
            var adjFromAmount = BigDecimal.ZERO

            val removeAdjustmentTrades = { trade: BareTrade ->
                !tradeAdjuster.isAdjustmentTrade(trade).also { adjTrade ->
                    if (adjTrade) {
                        adjFromAmount += amountCalculator.fromAmount(m.orderType, trade)
                    }
                }
            }

            m.trades.toVavrStream().filter(removeAdjustmentTrades).forEach { trade ->
                val amount = amountCalculator.targetAmount(m.orderType, trade)

                if (amount <= targetAmount) {
                    committedTrades.add(trade)
                    targetAmount -= amount
                } else {
                    if (targetAmount <= BigDecimal.ZERO) {
                        updatedTrades.add(trade)
                    } else {
                        val (l, r) = splitTrade(AmountType.Target, m.orderType, targetAmount, trade)
                        updatedTrades.addAll(l.filter(removeAdjustmentTrades))
                        committedTrades.addAll(r.filter(removeAdjustmentTrades))

                        targetAmount = r.toVavrStream()
                            .filter { tradeAdjuster.isAdjustmentTrade(it) }
                            .map { amountCalculator.targetAmount(m.orderType, it) }
                            .foldLeft(BigDecimal.ZERO) { a, b -> a + b }
                    }
                }
            }

            if (targetAmount.compareTo(BigDecimal.ZERO) != 0) {
                committedTrades.add(tradeAdjuster.adjustTargetAmount(targetAmount, m.orderType))
            }

            val updatedMarketTargetAmountDelta = run {
                val currentUpdatedMarketTargetAmount = tranIntentMarketExtensions.targetAmount(updatedTrades, m.orderType)
                val nextUpdatedMarketFromAmount = when (val market = updatedMarkets[i + 1]) {
                    is TranIntentMarketCompleted -> tranIntentMarketExtensions.fromAmount(market)
                    is TranIntentMarketPartiallyCompleted -> market.fromAmount
                    else -> throw RuntimeException("Other market types are not supported")
                }
                nextUpdatedMarketFromAmount - currentUpdatedMarketTargetAmount
            }

            if (updatedMarketTargetAmountDelta.compareTo(BigDecimal.ZERO) != 0) {
                updatedTrades.add(tradeAdjuster.adjustTargetAmount(updatedMarketTargetAmountDelta, m.orderType))
            }

            if (adjFromAmount.compareTo(BigDecimal.ZERO) != 0) {
                val updatedTradesFromAmount = tranIntentMarketExtensions.fromAmount(updatedTrades, m.orderType)
                val committedTradesFromAmount = tranIntentMarketExtensions.fromAmount(committedTrades, m.orderType)
                val updatedTradesTargetAmount = tranIntentMarketExtensions.targetAmount(updatedTrades, m.orderType)
                val committedTradesTargetAmount = tranIntentMarketExtensions.targetAmount(committedTrades, m.orderType)

                val percent = updatedTradesTargetAmount.divide(updatedTradesTargetAmount + committedTradesTargetAmount, 8, RoundingMode.DOWN)

                val a = (adjFromAmount * percent).setScale(8, RoundingMode.HALF_EVEN)
                val b = adjFromAmount - a

                val x = updatedTradesFromAmount + a
                val y = committedTradesFromAmount + b

                val d = when {
                    x < BigDecimal.ZERO -> -x
                    y < BigDecimal.ZERO -> y
                    else -> BigDecimal.ZERO
                }

                val xx = a - d
                val yy = b + d

                if (updatedTradesFromAmount + xx < BigDecimal.ZERO || committedTradesFromAmount + yy < BigDecimal.ZERO) {
                    throw RuntimeException("From amount can't be negative $updatedTradesFromAmount + $xx , $committedTradesFromAmount + $yy")
                }

                if (xx.compareTo(BigDecimal.ZERO) != 0) updatedTrades.add(tradeAdjuster.adjustFromAmount(xx))
                if (yy.compareTo(BigDecimal.ZERO) != 0) committedTrades.add(tradeAdjuster.adjustFromAmount(yy))
            }

            val updated = TranIntentMarketCompleted(
                m.market,
                m.orderSpeed,
                m.fromCurrencyType,
                Array.ofAll(updatedTrades)
            )

            val committed = TranIntentMarketCompleted(
                m.market,
                m.orderSpeed,
                m.fromCurrencyType,
                Array.ofAll(committedTrades)
            )

            updatedMarkets = updatedMarkets.update(i, updated)
            committedMarkets = committedMarkets.update(i, committed)

            i--
        }

        return tuple(updatedMarkets, committedMarkets)
    }
}
