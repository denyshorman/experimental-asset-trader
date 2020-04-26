package com.gitlab.dhorman.cryptotrader.trader.algo

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.PoloniexAdverseBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.CurrencyType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedPoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.trader.core.PoloniexTradeAdjuster
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketExtensions
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPredicted
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
