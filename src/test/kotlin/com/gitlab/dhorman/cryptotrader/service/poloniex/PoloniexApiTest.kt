package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.PoloniexBuySellAmountCalculator
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.fromAmountBuy
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.quoteAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.BuyOrderType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.ChartDataCandlestickPeriod
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.random.Random

@SpringBootTest
@ActiveProfiles(profiles = ["test"])
@Disabled
class PoloniexApiTest {
    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var poloniexApi: PoloniexApi

    @Autowired
    private lateinit var amountCalculator: PoloniexBuySellAmountCalculator

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
}
