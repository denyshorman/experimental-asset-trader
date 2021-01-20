package com.gitlab.dhorman.cryptotrader.entrypoint.crossexchangearbitragejavafxui

import com.gitlab.dhorman.cryptotrader.exchangesdk.binancefutures.BinanceFuturesApi
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexfutures.PoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.BinanceFuturesMarket
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.PoloniexFuturesMarket
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheableBinanceFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.cache.service.CacheablePoloniexFuturesApi
import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.CrossExchangeTrader
import java.math.BigDecimal

fun createCrossExchangeTrader(): CrossExchangeTrader {
    val poloniexMarket = "BTCUSDTPERP"
    val binanceMarket = "btcusdt"

    val poloniexFuturesApi = PoloniexFuturesApi.createApi()
    val binanceFuturesApi = BinanceFuturesApi.createMainNetApi()

    val poloniexFuturesApiCache = CacheablePoloniexFuturesApi(poloniexFuturesApi)
    val binanceFuturesApiCache = CacheableBinanceFuturesApi(binanceFuturesApi)

    val poloniexFuturesMarket = PoloniexFuturesMarket(
        cacheablePoloniexFuturesApi = poloniexFuturesApiCache,
        market = poloniexMarket,
    )

    val binanceFuturesMarket = BinanceFuturesMarket(
        cacheableBinanceFuturesApi = binanceFuturesApiCache,
        market = binanceMarket,
    )

    val trader = CrossExchangeTrader(
        leftMarket = poloniexFuturesMarket,
        rightMarket = binanceFuturesMarket,
        maxOpenCost = BigDecimal("100"),
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        trader.close()
        binanceFuturesMarket.close()
        poloniexFuturesMarket.close()
        binanceFuturesApiCache.close()
        poloniexFuturesApiCache.close()
    })

    return trader
}
