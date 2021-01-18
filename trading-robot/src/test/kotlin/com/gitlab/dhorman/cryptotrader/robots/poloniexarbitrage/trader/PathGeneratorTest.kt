package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.core.PoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.*
import io.vavr.collection.Array
import io.vavr.kotlin.tuple
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PathGeneratorTest {
    private lateinit var poloniexApi: ExtendedPoloniexApi
    private lateinit var amountCalculator: PoloniexBuySellAmountCalculator

    @Test
    fun `Test generated metrics for specific path`() {
        runBlocking {
            val fromCurrency = "USDT"
            val fromCurrencyAmount = BigDecimal("100")
            val feeMultiplier = FeeMultiplier(BigDecimal("0.99910000"), BigDecimal("0.99910000"))
            val orderBooks = poloniexApi.orderBooksPollingStream.first()
            val tradeVolumeStat = poloniexApi.tradeVolumeStat.first()

            val simulatedPath = SimulatedPath(
                Array.of(
                    SimulatedPath.OrderIntent(Market("USDT", "AVA"), OrderSpeed.Delayed),
                    SimulatedPath.OrderIntent(Market("TRX", "AVA"), OrderSpeed.Delayed),
                    SimulatedPath.OrderIntent(Market("BTC", "TRX"), OrderSpeed.Instant),
                    SimulatedPath.OrderIntent(Market("DAI", "BTC"), OrderSpeed.Delayed)
                )
            )

            val targetAmount = simulatedPath.targetAmount(fromCurrency, fromCurrencyAmount, feeMultiplier, orderBooks, amountCalculator)
            val waitTime = simulatedPath.waitTime(fromCurrency, tradeVolumeStat, arrayOf(tuple(fromCurrencyAmount, targetAmount)))

            println("Target amount: $targetAmount")
            println("Wait time: $waitTime")
        }
    }
}
