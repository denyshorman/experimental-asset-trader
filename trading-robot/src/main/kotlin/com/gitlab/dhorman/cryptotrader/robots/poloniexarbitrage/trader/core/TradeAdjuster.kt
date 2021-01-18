package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.core

import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.BareTrade
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Amount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderType

interface TradeAdjuster {
    fun isAdjustmentTrade(trade: BareTrade): Boolean
    fun adjustFromAmount(amount: Amount): BareTrade
    fun adjustTargetAmount(amount: Amount, orderType: OrderType): BareTrade
}
