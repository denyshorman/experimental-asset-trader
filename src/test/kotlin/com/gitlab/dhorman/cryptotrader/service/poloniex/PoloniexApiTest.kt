package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.PoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.fromAmountBuy
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.quoteAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.random.Random

@SpringBootTest
class PoloniexApiTest {
    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var poloniexApi: PoloniexApi

    @Autowired
    private lateinit var amountCalculator: PoloniexBuySellAmountCalculator

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Test
    fun `Subscribe to account notifications`() = runBlocking {
        poloniexApi.accountNotificationStream.collect {
            logger.info(it.toString())
        }
    }

    @Test
    fun `Cancel all orders`() {
        runBlocking {
            poloniexApi.cancelAllOrders(Market("USDT", "PAX"))
        }
    }

    @Test
    fun `Cancel not exiting order with client order id`() = runBlocking {
        try {
            val res = poloniexApi.cancelOrder(1, CancelOrderIdType.Client)
            println(res)
        } catch (e: Throwable) {
            println(e.message)
        }
    }

    @Test
    fun `Place order then move order and then cancel order`() = runBlocking(Dispatchers.Default) {
        launch {
            poloniexApi.accountNotificationStream.collect {
                println(it)
            }
        }

        launch {
            val market = Market("USDT", "ETH")
            val price = BigDecimal("100")
            val amount = BigDecimal("1")
            val clientOrderId = 0L
            val res = poloniexApi.placeLimitOrder(market, OrderType.Buy, price, amount, BuyOrderType.PostOnly, clientOrderId)
            println(res)
            val res2 = poloniexApi.moveOrder(res.orderId, price + BigDecimal("0.00000001"), null, BuyOrderType.PostOnly, 1L)
            println(res2)
            val res3 = poloniexApi.cancelOrder(res2.clientOrderId!!, CancelOrderIdType.Client)
            println(res3)
        }

        Unit
    }

    @Test
    fun `Place order then get open orders and then cancel order`() = runBlocking(Dispatchers.Default) {
        launch {
            poloniexApi.accountNotificationStream.collect {
                println("notifications: $it")
            }
        }

        launch {
            val market = Market("USDT", "ETH")
            val price = BigDecimal("100")
            val amount = BigDecimal("1")
            val clientOrderId = Instant.now().toEpochMilli()
            val res = poloniexApi.placeLimitOrder(market, OrderType.Buy, price, amount, BuyOrderType.PostOnly, clientOrderId)
            println("limit order result: $res")
            val res2 = poloniexApi.openOrders(market)
            println("open orders: $res2")
            val res3 = poloniexApi.cancelOrder(clientOrderId, CancelOrderIdType.Client)
            println("cancel order $res3")
        }

        Unit
    }

    @Test
    fun `Get open orders for market USDC_ATOM`() = runBlocking {
        val openOrders = poloniexApi.openOrders(Market("USDC", "ATOM"))
        logger.info("Open orders received: $openOrders")
    }

    @Test
    fun `Buy USDT on USDC_USDC market`() = runBlocking {
        val price = BigDecimal("0.9966")
        val baseAmount = BigDecimal("10.14628578")
        val quoteAmount = amountCalculator.quoteAmount(baseAmount, price)
        val buyResult = poloniexApi.placeLimitOrder(
            Market("USDC", "USDT"),
            OrderType.Buy,
            price,
            quoteAmount,
            BuyOrderType.FillOrKill
        )

        logger.info("Buy status $buyResult")
    }

    @Test
    fun `Get all balances`() = runBlocking {
        val balances = poloniexApi.completeBalances()
        logger.info("Complete balances $balances")
    }

    @Test
    fun `Get trade private history`() = runBlocking {
        val history = poloniexApi.tradeHistory(limit = 20, fromTs = Instant.now().minus(80, ChronoUnit.DAYS))
        logger.info("History $history")
    }

    @Test
    fun `Listen for account notifications`() = runBlocking {
        poloniexApi.accountNotificationStream.collect {
            println(it)
        }
    }

    @Test
    fun `Get order trades`() = runBlocking {
        val trades = poloniexApi.orderTrades(1648752843)
        println(trades)
    }

    @Test
    fun `Buy test`() = runBlocking {
        val initAmount = BigDecimal("1")

        while (true) {
            val price = BigDecimal(
                "" +
                        sequence { for (i in 0..3) yield(Random.nextInt(1, 7)) }.joinToString("") +
                        "." +
                        sequence { for (i in 0..7) yield(Random.nextInt(0, 10)) }.joinToString("")
            )
            val q = amountCalculator.quoteAmount(initAmount, price)

            val b = amountCalculator.fromAmountBuy(q, price)
            if (b.compareTo(BigDecimal.ONE) != 0) continue

            try {
                logger.info("Placing order: $price, $q")
                val res = poloniexApi.placeLimitOrder(Market("USDT", "BTC"), OrderType.Buy, price, q, BuyOrderType.PostOnly)
                poloniexApi.cancelOrder(res.orderId)
            } catch (e: Throwable) {
                logger.error("${e.message}: $price, $q, ${amountCalculator.fromAmountBuy(q, price)}")
            }
        }
    }

    @Test
    fun `Fetch candlestick chart data for TRX_WIN for 12-04-2020 - 13-04-2020 period with 5 min interval`() = runBlocking {
        val market = Market("TRX", "WIN")
        val fromTs = LocalDateTime.of(2020, 4, 12, 0, 0, 0).toInstant(ZoneOffset.UTC)
        val toTs = LocalDateTime.of(2020, 4, 13, 0, 0, 0).toInstant(ZoneOffset.UTC)
        val period = ChartDataCandlestickPeriod.PERIOD_5_MIN
        val data = poloniexApi.candlestickChartData(market, fromTs, toTs, period)

        logger.info(data.toString())
    }

    @Test
    fun `Fetch candlestick chart data for USDT_BTC for 12-04-2020 - 13-04-2020 period with 30 min interval`() = runBlocking {
        val market = Market("USDT", "BTC")
        val fromTs = LocalDateTime.of(2020, 4, 12, 0, 0, 0).toInstant(ZoneOffset.UTC)
        val toTs = LocalDateTime.of(2020, 4, 13, 0, 0, 0).toInstant(ZoneOffset.UTC)
        val period = ChartDataCandlestickPeriod.PERIOD_30_MIN
        val data = poloniexApi.candlestickChartData(market, fromTs, toTs, period)

        logger.info(data.toString())
    }

    @Test
    fun `Get order book API call should run successfully`() = runBlocking {
        val orderBook = poloniexApi.orderBooks(depth = 0)
        println(orderBook)
    }

    @Test
    fun `Correctly parses websocket message`() {
        val msg1 = """[1, 123456,null]"""
        val msg2 = """[1, 123456,123456]"""
        val msg3 = """[1, 123456,"123456"]"""

        val o1 = objectMapper.readValue<OrderKilled>(msg1)
        val o2 = objectMapper.readValue<OrderKilled>(msg2)
        val o3 = objectMapper.readValue<OrderKilled>(msg3)

        println(o1)
        println(o2)
        println(o3)
    }

    @Test
    fun `Correctly parses move order result message`() {
        val msg1 = """{"orderNumber":1,"resultingTrades":{},"feeMultiplier":"0.1","market":"USDT_BTC","clientOrderId":null}"""
        val msg2 = """{"orderNumber":1,"resultingTrades":{},"feeMultiplier":"0.1","market":"USDT_BTC","clientOrderId":1}"""
        val msg3 = """{"orderNumber":1,"resultingTrades":{},"feeMultiplier":"0.1","market":"USDT_BTC","clientOrderId":"1"}"""

        val o1 = objectMapper.readValue<MoveOrderResult>(msg1)
        val o2 = objectMapper.readValue<MoveOrderResult>(msg2)
        val o3 = objectMapper.readValue<MoveOrderResult>(msg3)

        println(o1)
        println(o2)
        println(o3)
    }
}
