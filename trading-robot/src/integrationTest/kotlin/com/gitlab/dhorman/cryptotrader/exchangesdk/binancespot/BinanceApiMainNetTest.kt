package com.gitlab.dhorman.cryptotrader.exchangesdk.binancespot

import com.gitlab.dhorman.cryptotrader.util.Secrets
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

class BinanceApiMainNetTest {
    private val binanceApi = BinanceApi(
        apiKey = Secrets.get("BINANCE_API_KEY")!!,
        apiSecret = Secrets.get("BINANCE_API_SECRET")!!,
        apiNet = BinanceApi.ApiNet.Main,
    )

    //region Wallet API
    @Test
    fun callSystemStatus() = runBlocking {
        val resp = binanceApi.systemStatus()
        println(resp)
    }

    @Test
    fun callGetUserCoins() = runBlocking {
        val resp = binanceApi.getUserCoins(Instant.now())
        println(resp)
    }

    @Test
    fun callTradeFee() = runBlocking {
        val tradeFee = binanceApi.tradeFee(Instant.now())
        println(tradeFee)
    }
    //endregion

    //region Market Data API
    @Test
    fun callPing() = runBlocking {
        val resp = binanceApi.ping()
        println(resp)
    }

    @Test
    fun callGetOrderBook() = runBlocking {
        val resp = binanceApi.getOrderBook("BTCUSDT")
        println(resp)
    }

    @Test
    fun callGetCandlestickData() = runBlocking {
        val resp = binanceApi.getCandlestickData(
            "BTCUSDT",
            BinanceApi.CandleStickInterval.INTERVAL_1_HOUR,
            LocalDateTime.of(2020, 7, 7, 0, 0, 0).toInstant(ZoneOffset.UTC),
            LocalDateTime.of(2020, 7, 7, 5, 0, 0).toInstant(ZoneOffset.UTC)
        )
        println(resp)
    }

    @Test
    fun callExchangeInfo() = runBlocking {
        val resp = binanceApi.getExchangeInfo()
        println(resp)
    }
    //endregion

    //region Spot Account/Trade API
    @Test
    fun callGetAccountInfo() = runBlocking {
        val resp = binanceApi.getAccountInfo(Instant.now())
        println(resp)
    }
    //endregion

    //region Market Streams API
    @Test
    fun subscribeToAggregateTradeStream() = runBlocking {
        binanceApi.aggregateTradeStream("btcusdt").collect { trade ->
            println(trade)
        }
    }

    @Test
    fun subscribeToTradeStream() = runBlocking {
        binanceApi.tradeStream("btcusdt").collect { trade ->
            println(trade)
        }
    }

    @Test
    fun subscribeToCandlestickStream() = runBlocking {
        binanceApi.candlestickStream("btcusdt", BinanceApi.CandleStickInterval.INTERVAL_1_MINUTE).collect { candlestick ->
            println(candlestick)
        }
    }

    @Test
    fun subscribeToIndividualSymbolMiniTickerStream() = runBlocking {
        binanceApi.individualSymbolMiniTickerStream("btcusdt").collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun subscribeToAllMarketMiniTickersStream() = runBlocking {
        binanceApi.allMarketMiniTickersStream.collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun subscribeToIndividualSymbolBookTickerStream() = runBlocking {
        binanceApi.individualSymbolBookTickerStream("usdtuah").collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun subscribeToAllBookTickerStream() = runBlocking {
        binanceApi.allBookTickerStream.collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun subscribeToPartialBookDepthStream() = runBlocking {
        binanceApi.partialBookDepthStream(
            "btcusdt",
            BinanceApi.PartialBookDepthEvent.Level.LEVEL_5,
            BinanceApi.BookUpdateSpeed.TIME_1000_MS
        ).collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun subscribeToOneStreamSimultaneously() = runBlocking {
        launch {
            binanceApi.individualSymbolMiniTickerStream("btcusdt").collect { ticker ->
                println("S1: $ticker")
            }
        }

        launch {
            binanceApi.individualSymbolMiniTickerStream("btcusdt").collect { ticker ->
                println("S2: $ticker")
            }
        }

        Unit
    }
    //endregion

    //region User Data Streams
    @Test
    fun subscribeToPrivateStream() = runBlocking {
        binanceApi.accountStream.collect { event ->
            println(event)
        }
    }
    //endregion
}
