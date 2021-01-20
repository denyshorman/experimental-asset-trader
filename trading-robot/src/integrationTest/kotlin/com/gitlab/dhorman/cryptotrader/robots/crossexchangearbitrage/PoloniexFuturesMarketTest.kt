package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexfutures.PoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheablePoloniexFuturesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PoloniexFuturesMarketTest {
    @Test
    fun openClosePositionTest() {
        runBlocking(Dispatchers.Default) {
            val cacheablePoloniexFuturesApi = CacheablePoloniexFuturesApi(PoloniexFuturesApi.createApi())
            val marketString = "XRPUSDTPERP"

            val market = PoloniexFuturesMarket(
                cacheablePoloniexFuturesApi = cacheablePoloniexFuturesApi,
                market = marketString,
            )

            val position = market.createMarketPosition(BigDecimal("10"), PositionSide.Long)
            assertEquals(FuturesMarketPositionState.Considered, position.state.value)

            position.open()
            assertEquals(FuturesMarketPositionState.Opened, position.state.value)
            println(position.profit.value)

            delay(60000)

            position.close()
            assertEquals(FuturesMarketPositionState.Closed, position.state.value)
            println(position.profit.value)
        }
    }
}
