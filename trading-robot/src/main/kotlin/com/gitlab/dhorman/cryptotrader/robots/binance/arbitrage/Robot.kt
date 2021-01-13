package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage

import com.gitlab.dhorman.cryptotrader.service.binance.BinanceApi
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

class Robot(private val binanceApi: BinanceApi) {
    fun start(scope: CoroutineScope) = scope.launch(CoroutineName("BinanceArbitrageRobot")) {

    }
}
