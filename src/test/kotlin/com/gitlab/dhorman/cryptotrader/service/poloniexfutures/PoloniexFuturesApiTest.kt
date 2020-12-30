package com.gitlab.dhorman.cryptotrader.service.poloniexfutures

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class PoloniexFuturesApiTest {
    @Autowired
    private lateinit var poloniexFuturesApi: PoloniexFuturesApi

    //region Market Data API
    @Test
    fun getOpenContracts() = runBlocking {
        val resp = poloniexFuturesApi.getOpenContracts()
        println(resp)
    }
    //endregion

    //region User API
    @Test
    fun getAccountOverview() = runBlocking {
        val resp = poloniexFuturesApi.getAccountOverview()
        println(resp)
    }
    //endregion

    //region WebSocket Private API
    @Test
    fun getPublicToken() = runBlocking {
        val resp = poloniexFuturesApi.getPublicToken()
        println(resp)
    }

    @Test
    fun getPrivateToken() = runBlocking {
        val resp = poloniexFuturesApi.getPrivateToken()
        println(resp)
    }
    //endregion

    //region Market Streams API
    @Test
    fun tickerStream() = runBlocking {
        poloniexFuturesApi.tickerStream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }

    @Test
    fun level2OrderBookStream() = runBlocking {
        poloniexFuturesApi.level2OrderBookStream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }

    @Test
    fun executionStream() = runBlocking {
        poloniexFuturesApi.executionStream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }

    @Test
    fun level3OrdersTradesStream() = runBlocking {
        poloniexFuturesApi.level3OrdersTradesStream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }

    @Test
    fun level2Depth5Stream() = runBlocking {
        poloniexFuturesApi.level2Depth5Stream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }

    @Test
    fun level2Depth50Stream() = runBlocking {
        poloniexFuturesApi.level2Depth50Stream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }

    @Test
    fun contractMarketDataStream() = runBlocking {
        poloniexFuturesApi.contractMarketDataStream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }

    @Test
    fun announcementStream() = runBlocking {
        poloniexFuturesApi.announcementStream.collect { event ->
            println(event)
        }
    }

    @Test
    fun tranStatsStream() = runBlocking {
        poloniexFuturesApi.tranStatsStream("BTCUSDTPERP").collect { event ->
            println(event)
        }
    }
    //endregion

    //region User Stream API
    // TODO: Add tests
    //endregion
}
