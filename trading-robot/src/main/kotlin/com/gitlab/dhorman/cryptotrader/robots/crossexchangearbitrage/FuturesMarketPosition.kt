package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import kotlinx.coroutines.flow.StateFlow
import java.math.BigDecimal

interface FuturesMarketPosition {
    val market: String
    val state: StateFlow<FuturesMarketPositionState>
    val side: PositionSide
    val quoteAmount: BigDecimal
    val profit: StateFlow<BigDecimal>

    suspend fun open()
    suspend fun close()
}
