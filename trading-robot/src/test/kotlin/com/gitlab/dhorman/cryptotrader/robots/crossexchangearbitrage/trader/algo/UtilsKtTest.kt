package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.algo

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.OrderBook
import io.vavr.collection.TreeMap
import io.vavr.kotlin.tuple
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class UtilsKtTest {
    @Test
    fun calculateFillAllMarketOrderTest() {
        val leftOrderBook = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal("36559.0"), BigDecimal("1.1370")),
                tuple(BigDecimal("36560.0"), BigDecimal("2.4450")),
                tuple(BigDecimal("36563.0"), BigDecimal("1.1070")),
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal("36558.0"), BigDecimal("0.2500")),
                tuple(BigDecimal("36545.0"), BigDecimal("0.6820")),
                tuple(BigDecimal("36534.0"), BigDecimal("1.0830")),
            ),
        )

        val rightOrderBook = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal("36533.01"), BigDecimal("0.043")),
                tuple(BigDecimal("36535.49"), BigDecimal("0.016")),
                tuple(BigDecimal("36535.50"), BigDecimal("0.076")),
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal("36533.00"), BigDecimal("0.032")),
                tuple(BigDecimal("36532.04"), BigDecimal("0.028")),
                tuple(BigDecimal("36532.03"), BigDecimal("0.436")),
            ),
        )

        val result = calculateFillAllMarketOrder(
            leftOrderBook = leftOrderBook,
            rightOrderBook = rightOrderBook,
            leftFee = BigDecimal("0.00075"),
            rightFee = BigDecimal("0.000200"),
            leftBaseAssetPrecision = 8,
            rightBaseAssetPrecision = 8,
            amount = BigDecimal("1"),
        )

        assertNull(result.first)
        assertNull(result.second)
    }
}
