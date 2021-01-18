package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.core

import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.BareTrade
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.Amount
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderType
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class PoloniexTradeAdjuster : TradeAdjuster {
    override fun isAdjustmentTrade(trade: BareTrade): Boolean {
        return trade.price.compareTo(BigDecimal.ONE) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0
            || trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ONE) == 0
            || trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0
    }

    override fun adjustFromAmount(amount: Amount): BareTrade {
        return BareTrade(amount, BigDecimal.ONE, BigDecimal.ZERO)
    }

    override fun adjustTargetAmount(amount: Amount, orderType: OrderType): BareTrade {
        return if (orderType == OrderType.Buy) {
            BareTrade(amount, BigDecimal.ZERO, BigDecimal.ONE)
        } else {
            BareTrade(amount, BigDecimal.ZERO, BigDecimal.ZERO)
        }
    }
}
