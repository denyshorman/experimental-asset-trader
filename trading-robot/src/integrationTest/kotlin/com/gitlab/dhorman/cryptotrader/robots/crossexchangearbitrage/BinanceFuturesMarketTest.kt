package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.exchangesdk.binancefutures.BinanceFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheableBinanceFuturesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class BinanceFuturesMarketTest {
    @Test
    fun openClosePositionTest() {
        runBlocking(Dispatchers.Default) {
            val cacheableBinanceFuturesApi = CacheableBinanceFuturesApi(BinanceFuturesApi.createTestNetApi())

            val market = BinanceFuturesMarket(
                cacheableBinanceFuturesApi = cacheableBinanceFuturesApi,
                market = "btcusdt",
            )

            val position = market.createMarketPosition(BigDecimal("0.001"), PositionSide.Long)
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
