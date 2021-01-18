package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import com.gitlab.dhorman.cryptotrader.util.EventData
import kotlinx.coroutines.flow.Flow
import java.math.BigDecimal

interface FuturesMarket {
    val generalInfo: Flow<FuturesMarketGeneralInfo>
    val orderBook: Flow<EventData<OrderBook>>
    suspend fun createMarketPosition(quoteAmount: BigDecimal, positionSide: PositionSide): FuturesMarketPosition
}
