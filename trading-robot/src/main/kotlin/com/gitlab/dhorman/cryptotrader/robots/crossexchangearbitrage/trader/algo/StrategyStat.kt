package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.algo

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.CrossExchangeTrader
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.scan
import kotlinx.coroutines.flow.shareIn
import java.math.BigDecimal

class StrategyStat(trader: CrossExchangeTrader) : AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("StrategyStat"))

    val openStrategyStat = run {
        trader.openStrategy.scan(Stat()) { stat, coeffs ->
            Stat(
                k0Min = stat.k0Min.min(coeffs.k0),
                k0Max = stat.k0Max.max(coeffs.k0),
                k1Min = stat.k1Min.min(coeffs.k1),
                k1Max = stat.k1Max.max(coeffs.k1),
            )
        }.shareIn(scope, SharingStarted.Lazily, 1)
    }

    override fun close() {
        scope.cancel()
    }

    data class Stat(
        val k0Min: BigDecimal = Int.MAX_VALUE.toBigDecimal(),
        val k0Max: BigDecimal = Int.MIN_VALUE.toBigDecimal(),
        val k1Min: BigDecimal = Int.MAX_VALUE.toBigDecimal(),
        val k1Max: BigDecimal = Int.MIN_VALUE.toBigDecimal(),
    )
}
