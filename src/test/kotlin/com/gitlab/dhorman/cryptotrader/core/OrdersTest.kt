package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.TradeStatModels
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

    @Test
    fun `Orders getDelayedOrderReverse BUY should generate correct fromAmount and toAmount amounts`() {
        val market = Market("USD", "UAH")

        val orderBook = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal.ONE.setScale(12, RoundingMode.HALF_EVEN) / BigDecimal(26), BigDecimal(5000))
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal.ONE.setScale(12, RoundingMode.HALF_EVEN) / BigDecimal(25), BigDecimal(6000))
            )
        )

        val makerFeeMultiplier = BigDecimal(0.999)
        val targetCurrency = "UAH"
        val toAmount = BigDecimal(260.5)
        val stat = TradeStatModels.Trade2State.map(TradeStatModels.Trade2State.calcFull(Queue.empty()))

        val buyQuoteReverse = Orders.getDelayedOrderReverse(
            market,
            targetCurrency,
            toAmount,
            makerFeeMultiplier,
            orderBook,
            stat
        )!!

        val buyQuoteDirect = Orders.getDelayedOrder(
            market,
            targetCurrency,
            buyQuoteReverse.fromAmount,
            makerFeeMultiplier,
            orderBook,
            stat
        )!!

        logger.info(
            """
            Buy quote direct
            $buyQuoteDirect
            Buy quote reverse
            $buyQuoteReverse
            """.trimIndent()
        )

        assert(buyQuoteDirect == buyQuoteReverse)
    }

    @Test
    fun `Orders getDelayedOrderReverse SELL should generate correct fromAmount and toAmount amounts`() {
        val market = Market("USD", "UAH")

        val orderBook = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal.ONE.setScale(12, RoundingMode.HALF_EVEN) / BigDecimal(26), BigDecimal(5000))
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal.ONE.setScale(12, RoundingMode.HALF_EVEN) / BigDecimal(25), BigDecimal(6000))
            )
        )

        val makerFeeMultiplier = BigDecimal(0.998)
        val targetCurrency = "USD"
        val toAmount = BigDecimal(125)
        val stat = TradeStatModels.Trade2State.map(TradeStatModels.Trade2State.calcFull(Queue.empty()))

        val sellQuoteReverse = Orders.getDelayedOrderReverse(
            market,
            targetCurrency,
            toAmount,
            makerFeeMultiplier,
            orderBook,
            stat
        )!!

        val sellQuoteDirect = Orders.getDelayedOrder(
            market,
            targetCurrency,
            sellQuoteReverse.fromAmount,
            makerFeeMultiplier,
            orderBook,
            stat
        )!!

        logger.info(
            """
         |Sell quote direct
         |$sellQuoteDirect
         |Sell quote reverse
         |$sellQuoteReverse
       """.trimMargin()
        )

        assert(sellQuoteDirect == sellQuoteReverse)
    }

    @Test
    fun `Orders path USDT_ETH - USDC_ETH should generate correct fromAmount and toAmount amounts`() {
        val marketUsdtEth = Market("USDT", "ETH")
        val marketUsdcEth = Market("USDC", "ETH")

        val orderBookUsdtEth = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal(115), BigDecimal(5000))
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal(114), BigDecimal(6000))
            )
        )

        val orderBookUsdcEth = OrderBook(
            asks = TreeMap.ofEntries(
                tuple(BigDecimal(118), BigDecimal(5000))
            ),
            bids = TreeMap.ofEntries(
                compareByDescending { it },
                tuple(BigDecimal(117), BigDecimal(6000))
            )
        )

        val makerFeeMultiplier = BigDecimal(0.999)
        val stat = TradeStatModels.Trade2State.map(TradeStatModels.Trade2State.calcFull(Queue.empty()))

        val targetUsdcAmount = 87.toBigDecimal()

        val sellEth = Orders.getDelayedOrderReverse(
            marketUsdcEth,
            "USDC",
            targetUsdcAmount,
            makerFeeMultiplier,
            orderBookUsdcEth,
            stat
        )!!

        val buyEth = Orders.getDelayedOrderReverse(
            marketUsdtEth,
            "ETH",
            sellEth.fromAmount,
            makerFeeMultiplier,
            orderBookUsdtEth,
            stat
        )!!

        logger.info(
            """
         |Path USDT_ETH -> USDC_ETH  =>  USDT -> USDC
         |Buy ETH on market USDT_ETH
         |$buyEth
         |Sell ETH on market USDC_ETH
         |$sellEth
       """.trimMargin()
        )

        assert((targetUsdcAmount.setScale(12, RoundingMode.HALF_EVEN) / buyEth.fromAmount).setScale(8, RoundingMode.FLOOR) == BigDecimal(1.03301857))
    }
}
