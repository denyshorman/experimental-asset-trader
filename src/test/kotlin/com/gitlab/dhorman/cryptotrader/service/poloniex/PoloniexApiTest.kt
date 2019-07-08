package com.gitlab.dhorman.cryptotrader.service.poloniex

import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.calcQuoteAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.BuyOrderType
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Ignore
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.math.BigDecimal

@SpringBootTest
@ActiveProfiles(profiles = ["test"])
class PoloniexApiTest {
    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var poloniexApi: PoloniexApi

    @Test
    @Ignore
    fun `Get open orders for market USDC_ATOM`() = runBlocking {
        val openOrders = poloniexApi.openOrders(Market("USDC", "ATOM"))
        logger.info("Open orders received: $openOrders")
    }

    @Test
    @Ignore
    fun `Buy USDT on USDC_USDC market`() = runBlocking {
        val price = BigDecimal("0.9966")
        val baseAmount = BigDecimal("10.14628578")
        val quoteAmount = calcQuoteAmount(baseAmount, price)
        val buyResult = poloniexApi.buy(
            Market("USDC", "USDT"),
            price,
            quoteAmount,
            BuyOrderType.FillOrKill
        )

        logger.info("Buy status $buyResult")
    }

    @Test
    @Ignore
    fun `Get all balances`() = runBlocking {
        val balances = poloniexApi.completeBalances()
        logger.info("Complete balances $balances")
    }
}
