package com.gitlab.dhorman.cryptotrader.trader.core

import com.gitlab.dhorman.cryptotrader.core.BareTrade
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType

interface TradeAdjuster {
    fun isAdjustmentTrade(trade: BareTrade): Boolean
    fun adjustFromAmount(amount: Amount): BareTrade
    fun adjustTargetAmount(amount: Amount, orderType: OrderType): BareTrade
}
