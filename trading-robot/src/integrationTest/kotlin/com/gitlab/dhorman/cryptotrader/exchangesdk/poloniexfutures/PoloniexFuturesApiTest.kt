package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexfutures

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PoloniexFuturesApiTest {
    private val poloniexFuturesApi = PoloniexFuturesApi.createApi()

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

    //region Trade API
    @Test
    fun placeOrder() {
        runBlocking {
            val resp = poloniexFuturesApi.placeOrder(
                PoloniexFuturesApi.PlaceOrderReq(
                    symbol = "XRPUSDTPERP",
                    clientOid = "x000",
                    remark = "test_order",
                    type = PoloniexFuturesApi.PlaceOrderReq.Type.Limit(
                        price = BigDecimal("0.29"),
                        amount = PoloniexFuturesApi.PlaceOrderReq.Type.Amount.Contract(size = 1),
                        timeInForce = PoloniexFuturesApi.TimeInForce.GoodTillCancel,
                        postOnly = true,
                        hidden = false,
                    ),
                    openClose = PoloniexFuturesApi.PlaceOrderReq.OpenClose.Open(
                        side = PoloniexFuturesApi.OrderSide.Buy,
                        leverage = BigDecimal.ONE,
                    ),
                )
            )

            println(resp)
        }
    }

    @Test
    fun cancelOrder() {
        runBlocking {
            val resp = poloniexFuturesApi.cancelOrder("5ff8985535af810006ea0f39")

            println(resp)
        }
    }

    @Test
    fun getOrders() {
        runBlocking {
            val resp = poloniexFuturesApi.getOrders()

            println(resp)
        }
    }

    @Test
    fun getPositions() {
        runBlocking {
            val resp = poloniexFuturesApi.getPositions()

            println(resp)
        }
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


    @Test
    fun privateMessagesStream() {
        runBlocking(Dispatchers.Default) {
            val symbol = "XRPUSDTPERP"

            launch {
                poloniexFuturesApi.privateMessagesStream.collect {
                    println("privateMessagesStream: $it")
                }
            }

            launch {
                poloniexFuturesApi.advancedOrdersStream.collect {
                    println("advancedOrdersStream: $it")
                }
            }

            launch {
                poloniexFuturesApi.walletStream.collect {
                    println("walletStream: $it")
                }
            }

            launch {
                poloniexFuturesApi.positionChangesStream(symbol).collect {
                    println("positionChangesStream: $it")
                }
            }
        }
    }
    //endregion
}
