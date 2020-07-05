package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.PoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.util.first
import io.vavr.collection.Array
import io.vavr.kotlin.tuple
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal

@SpringBootTest
class PathGeneratorTest {
    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var pathGenerator: PathGenerator

    @Autowired
    private lateinit var poloniexApi: ExtendedPoloniexApi

    @Autowired
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
