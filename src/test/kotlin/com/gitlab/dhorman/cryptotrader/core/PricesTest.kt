package com.gitlab.dhorman.cryptotrader.core

import org.junit.jupiter.api.Test

class PricesTest {
    @Test
    fun `Prices should correctly cut and add 1 to the end`() {
        val price = 3.123456789.toBigDecimal()
        val res = price.cut8add1
        assert(res == 3.12345679.toBigDecimal())
    }
}
