package com.gitlab.dhorman.cryptotrader.exchangesdk.binancefutures

import com.gitlab.dhorman.cryptotrader.util.Secrets
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class BinanceFuturesMainNetTest {
    private val binanceFuturesApi = BinanceFuturesApi(
        apiKey = Secrets.get("BINANCE_FUTURES_API_KEY")!!,
        apiSecret = Secrets.get("BINANCE_FUTURES_API_SECRET")!!,
        apiNet = BinanceFuturesApi.ApiNet.Main,
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
    //endregion

    //region Market Streams
    @Test
    fun aggregateTradeStream() = runBlocking {
        binanceFuturesApi.aggregateTradeStream("btcusdt").collect {
            println(it)
        }
    }

    @Test
    fun tradeStream() = runBlocking {
        binanceFuturesApi.tradeStream("btcusdt").collect {
            println(it)
        }
    }

    @Test
    fun markPriceStream() = runBlocking {
        binanceFuturesApi.markPriceStream("btcusdt", BinanceFuturesApi.MarkPriceUpdateSpeed.TIME_1_SEC).collect {
            println(it)
        }
    }

    @Test
    fun allMarketsMarkPriceStream() = runBlocking {
        binanceFuturesApi.allMarketsMarkPriceStream(BinanceFuturesApi.MarkPriceUpdateSpeed.TIME_1_SEC).collect {
            println(it)
        }
    }

    @Test
    fun candlestickStream() = runBlocking {
        binanceFuturesApi.candlestickStream("btcusdt", BinanceFuturesApi.CandleStickInterval.INTERVAL_1_MINUTE).collect { candlestick ->
            println(candlestick)
        }
    }

    @Test
    fun continuesContractCandlestickStream() = runBlocking {
        binanceFuturesApi.continuousContractCandlestickStream("btcusdt", BinanceFuturesApi.ContractType.PERPETUAL, BinanceFuturesApi.CandleStickInterval.INTERVAL_1_MINUTE)
            .collect { candlestick ->
                println(candlestick)
            }
    }

    @Test
    fun individualSymbolMiniTickerStream() = runBlocking {
        binanceFuturesApi.individualSymbolMiniTickerStream("btcusdt").collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun allMarketMiniTickersStream() = runBlocking {
        binanceFuturesApi.allMarketMiniTickersStream.collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun individualSymbolTickerStream() = runBlocking {
        binanceFuturesApi.individualSymbolTickerStream("btcusdt").collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun allMarketTickersStream() = runBlocking {
        binanceFuturesApi.allMarketTickersStream.collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun individualSymbolBookTickerStream() = runBlocking {
        binanceFuturesApi.individualSymbolBookTickerStream("btcusdt").collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun allBookTickerStream() = runBlocking {
        binanceFuturesApi.allBookTickerStream.collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun liquidationOrderStream() = runBlocking {
        binanceFuturesApi.liquidationOrderStream("btcusdt").collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun allMarketLiquidationOrderStream() = runBlocking {
        binanceFuturesApi.allMarketLiquidationOrderStream.collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun partialBookDepthStream() = runBlocking {
        binanceFuturesApi.partialBookDepthStream(
            "btcusdt",
            BinanceFuturesApi.PartialBookDepthEvent.Level.LEVEL_5,
            BinanceFuturesApi.BookUpdateSpeed.TIME_500_MS,
        ).collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun diffDepthStream() = runBlocking {
        binanceFuturesApi.diffDepthStream(
            "btcusdt",
            BinanceFuturesApi.BookUpdateSpeed.TIME_500_MS,
        ).collect { ticker ->
            println(ticker)
        }
    }

    @Test
    fun compositeIndexStream() = runBlocking {
        binanceFuturesApi.compositeIndexStream("defiusdt").collect { ticker ->
            println(ticker)
        }
    }
    //endregion
}
