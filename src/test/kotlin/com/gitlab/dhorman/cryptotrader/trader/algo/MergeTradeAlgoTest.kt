package com.gitlab.dhorman.cryptotrader.trader.algo

import com.gitlab.dhorman.cryptotrader.core.BareTrade
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.core.OrderSpeed
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.PoloniexAdverseBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.CurrencyType
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.core.PoloniexTradeAdjuster
import com.gitlab.dhorman.cryptotrader.trader.model.*
import com.nhaarman.mockitokotlin2.mock
import io.vavr.collection.Array
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals

class MergeTradeAlgoTest {
    private val amountCalculator = AdjustedPoloniexBuySellAmountCalculator(PoloniexAdverseBuySellAmountCalculator())
    private val tradeAdjuster = PoloniexTradeAdjuster()
    private val tranIntentMarketExtensions = TranIntentMarketExtensions(amountCalculator, mock())
    private val mergeTradeAlgo = MergeTradeAlgo(amountCalculator, tradeAdjuster, tranIntentMarketExtensions)

    @Test
    fun `Test from and target amount for first market after merge`() {
        val markets: Array<TranIntentMarket> = Array.of(
            TranIntentMarketCompleted(
                market = Market("USDC", "JST"), orderSpeed = OrderSpeed.Delayed, fromCurrencyType = CurrencyType.Base, trades = Array.of(
                    BareTrade(BigDecimal("162.52050742"), BigDecimal("0.00840005"), BigDecimal("0.99910000")),
                    BareTrade(BigDecimal("238.09382087"), BigDecimal("0.00819524"), BigDecimal("0.99910000")),
                    BareTrade(BigDecimal("238.09382086"), BigDecimal("0.00840005"), BigDecimal("0.99910000")),
                    BareTrade(BigDecimal("378.34114438"), BigDecimal("0.00840005"), BigDecimal("0.99910000")),
                    BareTrade(BigDecimal("3E-8"), BigDecimal("0"), BigDecimal("1")),
                    BareTrade(BigDecimal("0.00000134"), BigDecimal("1"), BigDecimal("0"))
                )
            ),
            TranIntentMarketPartiallyCompleted(
                market = Market("TRX", "JST"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Quote,
                fromAmount = BigDecimal("8.49450227")
            ),
            TranIntentMarketPredicted(
                market = Market("USDC", "TRX"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Quote
            )
        )

        val mergeInit = BigDecimal("2.00000000")
        val mergeCurrent = BigDecimal("237.87953643")
        val fromAmountBeforeMerge = tranIntentMarketExtensions.fromAmount(markets[0] as TranIntentMarketCompleted)
        val targetAmountBeforeMerge = tranIntentMarketExtensions.targetAmount(markets[0] as TranIntentMarketCompleted)
        val mergedMarkets = mergeTradeAlgo.mergeMarkets(markets, mergeInit, mergeCurrent)
        val fromAmountAfterMerge = tranIntentMarketExtensions.fromAmount(mergedMarkets[0] as TranIntentMarketCompleted)
        val targetAmountAfterMerge = tranIntentMarketExtensions.targetAmount(mergedMarkets[0] as TranIntentMarketCompleted)

        assertEquals(fromAmountBeforeMerge + mergeInit, fromAmountAfterMerge)
        assertEquals(targetAmountBeforeMerge + mergeCurrent, targetAmountAfterMerge)
    }

    @Test
    fun `Test from amount for first market after merge`() {
        val markets: Array<TranIntentMarket> = Array.of(
            TranIntentMarketPartiallyCompleted(
                market = Market("USDT", "TRX"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Base,
                fromAmount = BigDecimal("2.00000000")
            ),
            TranIntentMarketPredicted(
                market = Market("USDC", "TRX"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Quote
            )
        )

        val mergeInit = BigDecimal("2.00000000")
        val fromAmountBeforeMerge = (markets[0] as TranIntentMarketPartiallyCompleted).fromAmount
        val mergedMarkets = mergeTradeAlgo.mergeMarkets(markets, mergeInit, mergeInit)
        val fromAmountAfterMerge = (mergedMarkets[0] as TranIntentMarketPartiallyCompleted).fromAmount

        assertEquals(fromAmountBeforeMerge + mergeInit, fromAmountAfterMerge)
    }
}
