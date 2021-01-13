package com.gitlab.dhorman.cryptotrader.trader.algo

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.PoloniexAdverseBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.CurrencyType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.core.PoloniexTradeAdjuster
import com.gitlab.dhorman.cryptotrader.trader.model.*
import com.nhaarman.mockitokotlin2.mock
import io.vavr.collection.Array
import io.vavr.collection.Stream
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.toVavrStream
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class SplitTradeAlgoTest {
    private val amountCalculator = AdjustedPoloniexBuySellAmountCalculator(PoloniexAdverseBuySellAmountCalculator())
    private val tradeAdjuster = PoloniexTradeAdjuster()
    private val tranIntentMarketExtensions = TranIntentMarketExtensions(amountCalculator, mock())
    private val splitTradeAlgo = SplitTradeAlgo(amountCalculator, tradeAdjuster, tranIntentMarketExtensions)

    @ParameterizedTest
    @MethodSource("splitTradeAmountTypeOrderTypeProvider")
    fun `Test splitTrade method for arguments FromTarget, BuySell and random trade`(amountType: AmountType, orderType: OrderType) {
        val trade = run {
            val q = BigDecimal(abs(Random.nextDouble())).setScale(8, RoundingMode.DOWN) + BigDecimal(Random.nextInt(0, 100))
            val p = BigDecimal(abs(Random.nextDouble())).setScale(8, RoundingMode.DOWN) + BigDecimal(Random.nextInt(0, 100))
            val f = BigDecimal(abs(Random.nextDouble())).setScale(8, RoundingMode.DOWN) + BigDecimal(Random.nextInt(0, 100))
            BareTrade(q, p, f)
        }
        val fromAmountTrade = amountCalculator.fromAmount(orderType, trade)
        val targetAmountTrade = amountCalculator.targetAmount(orderType, trade)

        val clientAmount = run {
            val amount = (amountCalculator.targetAmount(orderType, trade) * BigDecimal("0.77")).setScale(8, RoundingMode.DOWN)
            (amount * BigDecimal("0.81")).setScale(8, RoundingMode.DOWN)
        }

        val (splitLeftTrade, splitRightTrades) = splitTradeAlgo.splitTrade(amountType, orderType, clientAmount, trade)

        val actualFromAmount = Stream.concat(splitLeftTrade, splitRightTrades)
            .map { amountCalculator.fromAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }

        val actualTargetAmount = Stream.concat(splitLeftTrade, splitRightTrades)
            .map { amountCalculator.targetAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }

        if (fromAmountTrade != actualFromAmount || targetAmountTrade != actualTargetAmount) {
            logger.info("splitTrade($amountType, $orderType, $clientAmount, $trade)")
            logger.info("$splitLeftTrade")
            logger.info("$splitRightTrades")
            logger.info("$fromAmountTrade $actualFromAmount")
            logger.info("$targetAmountTrade $actualTargetAmount")

            fail("Split trade algorithm failed")
        }
    }

    @Test
    fun `Split trades from and target amounts must be non negative`() {
        val amountType = AmountType.From
        val orderType = OrderType.Buy
        val trade = BareTrade(BigDecimal("0.78292478"), BigDecimal("281.10867894"), BigDecimal("0.99910000"))
        val tradeFromAmount = amountCalculator.fromAmount(orderType, trade)
        val tradeTargetAmount = amountCalculator.targetAmount(orderType, trade)
        val amount = BigDecimal("107.00848696")

        logger.info("tradeFromAmount = $tradeFromAmount")
        logger.info("tradeTargetAmount = $tradeTargetAmount")
        logger.info("splitTrade($amountType, $orderType, $amount, $trade)")

        val (leftTrades, rightTrades) = splitTradeAlgo.splitTrade(amountType, orderType, amount, trade)

        val fromAmountLeftTrades = leftTrades.toVavrStream()
            .map { amountCalculator.fromAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }
        val targetAmountLeftTrades = leftTrades.toVavrStream()
            .map { amountCalculator.targetAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }
        val fromAmountRightTrades = rightTrades.toVavrStream()
            .map { amountCalculator.fromAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }
        val targetAmountRightTrades = rightTrades.toVavrStream()
            .map { amountCalculator.targetAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }

        logger.info("fromAmountLeftTrades = $fromAmountLeftTrades")
        logger.info("targetAmountLeftTrades = $targetAmountLeftTrades")
        logger.info("fromAmountRightTrades = $fromAmountRightTrades")
        logger.info("targetAmountRightTrades = $targetAmountRightTrades")

        assertTrue(fromAmountLeftTrades >= BigDecimal.ZERO)
        assertTrue(targetAmountLeftTrades >= BigDecimal.ZERO)
        assertTrue(fromAmountRightTrades >= BigDecimal.ZERO)
        assertTrue(targetAmountRightTrades >= BigDecimal.ZERO)

        logger.info(leftTrades.toString())
        logger.info(rightTrades.toString())
    }

    @Test
    fun `Received trade must completely fill partially completed market`() {
        val amountType = AmountType.From
        val orderType = OrderType.Buy
        val fromAmount = BigDecimal("107.00848696")
        val receivedTrade = BareTrade(BigDecimal("0.78292478"), BigDecimal("281.10867894"), BigDecimal("0.99910000"))

        val markets = Array.of(
            TranIntentMarketPartiallyCompleted(
                market = Market("USDT", "BTC"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Base,
                fromAmount = fromAmount
            ),
            TranIntentMarketPredicted(
                market = Market("USDC", "BTC"),
                fromCurrencyType = CurrencyType.Quote,
                orderSpeed = OrderSpeed.Instant
            )
        )

        val (leftTrades, rightTrades) = splitTradeAlgo.splitTrade(amountType, orderType, fromAmount, receivedTrade)

        val fromAmountLeftTrades = leftTrades.toVavrStream()
            .map { amountCalculator.fromAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }
        val targetAmountLeftTrades = leftTrades.toVavrStream()
            .map { amountCalculator.targetAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }
        val fromAmountRightTrades = rightTrades.toVavrStream()
            .map { amountCalculator.fromAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }
        val targetAmountRightTrades = rightTrades.toVavrStream()
            .map { amountCalculator.targetAmount(orderType, it) }
            .foldLeft(BigDecimal.ZERO) { x, y -> x + y }

        assertTrue(fromAmountLeftTrades >= BigDecimal.ZERO)
        assertTrue(targetAmountLeftTrades >= BigDecimal.ZERO)
        assertTrue(fromAmountRightTrades >= BigDecimal.ZERO)
        assertTrue(targetAmountRightTrades >= BigDecimal.ZERO)

        val currMarketIdx = 0
        val (update, _) = splitTradeAlgo.splitMarkets(markets, currMarketIdx, rightTrades.toArray())

        val newFromAmount = (update[currMarketIdx] as TranIntentMarketPartiallyCompleted).fromAmount

        assertTrue(newFromAmount.compareTo(BigDecimal.ZERO) == 0)
    }

    @Test
    fun `Split markets with trade must return correct amounts`() {
        val markets: Array<TranIntentMarket> = Array.of(
            TranIntentMarketCompleted(
                market = Market("USDC", "USDT"), orderSpeed = OrderSpeed.Instant, fromCurrencyType = CurrencyType.Base, trades = Array.of(
                    BareTrade(quoteAmount = BigDecimal("0.00070626"), price = BigDecimal("1.00100000"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("0.09848358"), price = BigDecimal("1.00103865"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("0.70921680"), price = BigDecimal("0.94894326"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("1E-7"), price = BigDecimal("0"), feeMultiplier = BigDecimal("1")),
                    BareTrade(quoteAmount = BigDecimal("4E-8"), price = BigDecimal("1"), feeMultiplier = BigDecimal("0"))
                )
            ),
            TranIntentMarketPartiallyCompleted(
                market = Market("USDT", "SC"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Base,
                fromAmount = BigDecimal("0.80767907")
            ),
            TranIntentMarketPredicted(
                market = Market("USDC", "SC"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Quote
            )
        )

        val trade = BareTrade(BigDecimal("229.78469175"), BigDecimal("0.00217595"), BigDecimal("0.99910000"))

        val (update, commit) = splitTradeAlgo.splitMarkets(markets, 1, Array.of(trade))

        println(update)
        println(commit)

        assertEquals(tranIntentMarketExtensions.targetAmount(update[0] as TranIntentMarketCompleted), (update[1] as TranIntentMarketPartiallyCompleted).fromAmount)
        assertEquals(tranIntentMarketExtensions.targetAmount(commit[0] as TranIntentMarketCompleted), tranIntentMarketExtensions.fromAmount(commit[1] as TranIntentMarketCompleted))
        assertEquals(tranIntentMarketExtensions.targetAmount(commit[1] as TranIntentMarketCompleted), (commit[2] as TranIntentMarketPartiallyCompleted).fromAmount)
    }

    @Test
    fun `Correctly split markets by trade`() {
        val markets = Array.of(
            TranIntentMarketCompleted(
                market = Market("USDT", "STEEM"),
                orderSpeed = OrderSpeed.Instant,
                fromCurrencyType = CurrencyType.Base,
                trades = Array.of(
                    BareTrade(quoteAmount = BigDecimal("1E-8"), price = BigDecimal("0.20256151"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("1E-8"), price = BigDecimal("0.20256151"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("0.00602591"), price = BigDecimal("0.20256151"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("1.53335976"), price = BigDecimal("0.20256151"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("0.04357190"), price = BigDecimal("0.20258026"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("14.90336851"), price = BigDecimal("0.20258016"), feeMultiplier = BigDecimal("0.99910000")),
                    BareTrade(quoteAmount = BigDecimal("4E-8"), price = BigDecimal("0"), feeMultiplier = BigDecimal("1")),
                    BareTrade(quoteAmount = BigDecimal("1.9E-7"), price = BigDecimal("1"), feeMultiplier = BigDecimal("0"))
                )
            ),
            TranIntentMarketCompleted(
                market = Market("BTC", "STEEM"),
                orderSpeed = OrderSpeed.Instant,
                fromCurrencyType = CurrencyType.Quote,
                trades = Array.of(
                    BareTrade(quoteAmount = BigDecimal("5.43088303"), price = BigDecimal("0.00002163"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("9.46039833"), price = BigDecimal("0.00002162"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("0.00602049"), price = BigDecimal("0.00002162"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("1.53197974"), price = BigDecimal("0.00002162"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("0.04353269"), price = BigDecimal("0.00002162"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("1E-8"), price = BigDecimal("0"), feeMultiplier = BigDecimal("0")),
                    BareTrade(quoteAmount = BigDecimal("-0.00132588"), price = BigDecimal("1"), feeMultiplier = BigDecimal("0"))
                )
            ),
            TranIntentMarketCompleted(
                market = Market("BTC", "ETH"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Base,
                trades = Array.of(
                    BareTrade(quoteAmount = BigDecimal("0.01282798"), price = BigDecimal("0.02506702"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("0.00000492"), price = BigDecimal("0.02506702"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("0.00131948"), price = BigDecimal("0.02506702"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("0.00003746"), price = BigDecimal("0.02506702"), feeMultiplier = BigDecimal("0.99875000")),
                    BareTrade(quoteAmount = BigDecimal("3E-8"), price = BigDecimal("0"), feeMultiplier = BigDecimal("1"))
                )
            ),
            TranIntentMarketCompleted(
                market = Market("USDT", "ETH"),
                orderSpeed = OrderSpeed.Delayed,
                fromCurrencyType = CurrencyType.Quote,
                trades = Array.of(BareTrade(quoteAmount = BigDecimal("0.01417212"), price = BigDecimal("233.72317567"), feeMultiplier = BigDecimal("0.99875000")))
            ),
            TranIntentMarketCompleted(
                market = Market("USDT", "DAI"),
                orderSpeed = OrderSpeed.Instant,
                fromCurrencyType = CurrencyType.Base,
                trades = Array.of(BareTrade(quoteAmount = BigDecimal("3.30364745"), price = BigDecimal("1.00138180"), feeMultiplier = BigDecimal("0.99875000")))
            ),
            TranIntentMarketPartiallyCompleted(
                market = Market("DAI", "BTC"),
                orderSpeed = OrderSpeed.Instant,
                fromCurrencyType = CurrencyType.Base,
                fromAmount = BigDecimal("3.29951789")
            ),
            TranIntentMarketPredicted(market = Market("BTC", "STEEM"), orderSpeed = OrderSpeed.Instant, fromCurrencyType = CurrencyType.Base),
            TranIntentMarketPredicted(market = Market("USDT", "STEEM"), orderSpeed = OrderSpeed.Delayed, fromCurrencyType = CurrencyType.Quote)
        )

        val trade = BareTrade(BigDecimal("0.00035233"), BigDecimal("9364.68529055"), BigDecimal("0.99875000"))

        assertTrue { (markets[5] as TranIntentMarketPartiallyCompleted).fromAmount >= amountCalculator.fromAmountBuy(trade) }

        val (update, commit) = splitTradeAlgo.splitMarkets(markets, 5, Array.of(trade))

        println(update)
        println(commit)
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        @JvmStatic
        fun splitTradeAmountTypeOrderTypeProvider(): Stream<Arguments> {
            return Stream.of(
                arguments(AmountType.From, OrderType.Buy),
                arguments(AmountType.From, OrderType.Sell),
                arguments(AmountType.Target, OrderType.Buy),
                arguments(AmountType.Target, OrderType.Sell)
            )
        }
    }
}
