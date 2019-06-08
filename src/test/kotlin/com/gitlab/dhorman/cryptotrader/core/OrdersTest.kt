package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.data.tradestat.Trade2State
import io.vavr.collection.Queue
import io.vavr.collection.TreeMap
import io.vavr.kotlin.list
import io.vavr.kotlin.tuple
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.RoundingMode

class OrdersTest {
    private val logger = KotlinLogging.logger {}

    @Test
    fun `Orders getInstantOrder BUY should generate correct trades USDT - BTC`() {
        val market = Market("USDT", "BTC")

        val orderBook = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal(50), BigDecimal(1)),
                tuple(BigDecimal(60), BigDecimal(0.2))
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal(40), BigDecimal(1)),
                tuple(BigDecimal(30), BigDecimal(2))
            )
        )

        val fromAmount = 100.toBigDecimal()
        val toAmount = 1.1976.toBigDecimal()
        val fee = 0.998.toBigDecimal()

        val actual = Orders.getInstantOrder(
            market,
            market.quoteCurrency,
            fromAmount,
            fee,
            orderBook
        )!!

        val expected = InstantOrder(
            market,
            market.baseCurrency,
            market.quoteCurrency,
            fromAmount,
            toAmount,
            OrderType.Buy,
            0.01996.toBigDecimal(),
            0.011976.toBigDecimal(),
            38.toBigDecimal(),
            fee,
            list(
                InstantOrder.Companion.Trade(60.toBigDecimal(), 0.2.toBigDecimal()),
                InstantOrder.Companion.Trade(50.toBigDecimal(), 1.toBigDecimal())
            )
        )

        logger.info(
            """
         |USDT -> BTC
         |$expected
         |$actual
       """.trimMargin("|")
        )

        assert(actual == expected)
    }

    @Test
    fun `Orders getInstantOrder SELL should generate correct trades BTC - USDT`() {
        val market = Market("USDT", "BTC")

        val orderBook = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal(50), BigDecimal(1)),
                tuple(BigDecimal(60), BigDecimal(0.2))
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal(40), BigDecimal(1)),
                tuple(BigDecimal(30), BigDecimal(2))
            )
        )

        val fromAmount = BigDecimal(1.2)
        val toAmount = BigDecimal(45.9080)
        val fee = BigDecimal(0.998)

        val actual = Orders.getInstantOrder(
            market,
            market.baseCurrency,
            fromAmount,
            fee,
            orderBook
        )!!

        val expected = InstantOrder(
            market,
            market.quoteCurrency,
            market.baseCurrency,
            fromAmount,
            toAmount,
            OrderType.Sell,
            39.920.toBigDecimal(),
            toAmount.setScale(8, RoundingMode.HALF_EVEN) / fromAmount,
            0.toBigDecimal(),
            fee,
            list(
                InstantOrder.Companion.Trade(30.toBigDecimal(), 0.2.toBigDecimal()),
                InstantOrder.Companion.Trade(40.toBigDecimal(), 1.toBigDecimal())
            )
        )

        logger.info(
            """
         |BTC -> USDT
         |$expected
         |$actual
       """.trimMargin()
        )

        assert(actual == expected)
    }
}
