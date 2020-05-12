package com.gitlab.dhorman.cryptotrader.service.poloniex.core

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal

@SpringBootTest
class PoloniexAdverseBuySellAmountCalculatorTest {
    @Autowired
    private lateinit var calculator: PoloniexAdverseBuySellAmountCalculator

    @Test
    fun test() {
        val f0 = BigDecimal("2.19033450")
        val q = calculator.quoteAmount(BigDecimal("2.19033450"), BigDecimal("181.79392390"))
        val f = calculator.fromAmountBuy(q, BigDecimal("183.31756046"))
        assertTrue(f > f0)
    }
}
