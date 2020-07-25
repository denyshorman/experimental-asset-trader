package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component("BinanceAdjustedBuySellAmountCalculator")
open class AdjustedBuySellAmountCalculator(
    @Qualifier("BinanceBuySellAmountCalculator") private val amountCalculator: BuySellAmountCalculator
) : BuySellAmountCalculator() {
    override fun quoteAmount(data: Data): BigDecimal {
        return amountCalculator.quoteAmount(data)
    }

    override fun fromAmountBuy(data: Data): BigDecimal {
        return amountCalculator.fromAmountBuy(data)
    }

    override fun targetAmountBuy(data: Data): BigDecimal {
        return amountCalculator.targetAmountBuy(data)
    }

    override fun fromAmountSell(data: Data): BigDecimal {
        return if (data.price.compareTo(BigDecimal.ZERO) == 0 && data.feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
            BigDecimal.ZERO
        } else {
            amountCalculator.fromAmountSell(data)
        }
    }

    override fun targetAmountSell(data: Data): BigDecimal {
        return if (data.price.compareTo(BigDecimal.ZERO) == 0 && data.feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
            data.amount
        } else {
            amountCalculator.targetAmountSell(data)
        }
    }
}
