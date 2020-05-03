package com.gitlab.dhorman.cryptotrader.trader

import io.vavr.kotlin.stream
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal

@SpringBootTest
class IndicatorStreamsTest {

    @Autowired
    private lateinit var indicators: Indicators

    @Test
    fun `Correctly returns paths`() = runBlocking {
        val initAmount = BigDecimal(100)
        val paths = indicators.getPaths(
            "USDT",
            initAmount,
            stream("USDT", "USDC", "USDJ", "PAX"),
            fun(p): Boolean {
                val targetMarket = p.chain.lastOrNull() ?: return false
                return initAmount < targetMarket.toAmount
            },
            Comparator { p0, p1 ->
                if (p0.id == p1.id) {
                    0
                } else {
                    p1.profitability.compareTo(p0.profitability)
                }
            })

        println("paths found: ${paths.size()}")
        println("best path: ${paths.firstOrNull()}")
    }
}
