package com.gitlab.dhorman.cryptotrader.exchangesdk.binancefutures

import com.gitlab.dhorman.cryptotrader.util.Secrets
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class BinanceFuturesTestNetTest {
    private val binanceFuturesApi = BinanceFuturesApi(
        apiKey = Secrets.get("BINANCE_FUTURES_TEST_NET_API_KEY")!!,
        apiSecret = Secrets.get("BINANCE_FUTURES_TEST_NET_API_SECRET")!!,
        apiNet = BinanceFuturesApi.ApiNet.Test,
    )

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

    @Test
    fun getCurrentPositionMode() = runBlocking {
        val resp = binanceFuturesApi.getCurrentPositionMode()
        println(resp)
    }

    @Test
    fun newOrder() = runBlocking {
        val resp = binanceFuturesApi.placeNewOrder(
            "BTCUSDT",
            BinanceFuturesApi.OrderSide.SELL,
            BinanceFuturesApi.OrderType.LIMIT,
            timeInForce = BinanceFuturesApi.TimeInForce.POST_ONLY,
            price = BigDecimal("39600"),
            quantity = BigDecimal("0.001"),
        )
        println(resp)
    }

    @Test
    fun cancelOrder() = runBlocking {
        val resp = binanceFuturesApi.cancelOrder(
            "BTCUSDT",
            2607085085,
        )
        println(resp)
    }

    @Test
    fun getAllOrders() = runBlocking {
        val resp = binanceFuturesApi.getAllOrders("BTCUSDT")
        println(resp)
    }

    @Test
    fun getAllOpenOrders() = runBlocking {
        val resp = binanceFuturesApi.getAllOpenOrders(
            "BTCUSDT",
        )
        println(resp)
    }

    @Test
    fun getCurrentPositions() = runBlocking {
        val resp = binanceFuturesApi.getCurrentPositions(
            "BTCUSDT",
        )
        println(resp)
    }

    @Test
    fun changeInitialLeverage() = runBlocking {
        val resp = binanceFuturesApi.changeInitialLeverage(
            "BTCUSDT",
            1,
        )
        println(resp)
    }

    @Test
    fun changeMarginType() = runBlocking {
        val resp = binanceFuturesApi.changeMarginType(
            "BTCUSDT",
            BinanceFuturesApi.MarginType.CROSSED,
        )
        println(resp)
    }

    @Test
    fun getAccountBalance() = runBlocking {
        val resp = binanceFuturesApi.getAccountBalance()
        println(resp)
    }

    @Test
    fun getAccountInfo() = runBlocking {
        val resp = binanceFuturesApi.getAccountInfo()
        println(resp)
    }
    //endregion

    //region User Data Streams
    @Test
    fun accountStream() = runBlocking {
        binanceFuturesApi.accountStream.collect {
            println(it)
        }
    }
    //endregion
}
