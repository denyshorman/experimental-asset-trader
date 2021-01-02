package com.gitlab.dhorman.cryptotrader.service.binance

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class BinanceFuturesTestNetTest {
    @Autowired
    @Qualifier("binanceFuturesTestNetApi")
    private lateinit var binanceFuturesApi: BinanceFuturesApi

    //region Market Data API
    @Test
    fun ping() = runBlocking {
        val resp = binanceFuturesApi.ping()
        println(resp)
    }

    @Test
    fun getCurrentServerTime() = runBlocking {
        val resp = binanceFuturesApi.getCurrentServerTime()
        println(resp)
    }

    @Test
    fun getExchangeInfo() = runBlocking {
        val resp = binanceFuturesApi.getExchangeInfo()
        println(resp)
    }
    //endregion

    //region Account/Trades API
    @Test
    fun getCommissionRate() = runBlocking {
        val resp = binanceFuturesApi.getCommissionRate("BTCUSDT")
        println(resp)
    }
    //endregion
}
