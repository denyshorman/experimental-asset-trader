package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.core

import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode

@Primary
@Component
class PoloniexAdverseBuySellAmountCalculator : PoloniexBuySellAmountCalculator {
    // USDT -> ETH
    override fun quoteAmount(baseAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        var q = baseAmount.divide(price, 8, RoundingMode.UP)
        if (fromAmountBuy(q, price).compareTo(baseAmount) != 0) {
            q = baseAmount.divide(price, 8, RoundingMode.DOWN)
        }
        return q
    }

    // -USDT
    override fun fromAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return (quoteAmount * price).setScale(8, RoundingMode.UP)
    }

    // +ETH
    override fun targetAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return (quoteAmount * feeMultiplier).setScale(8, RoundingMode.DOWN)
    }

    // -ETH
    override fun fromAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return quoteAmount
    }

    // +USDT
    override fun targetAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return ((quoteAmount * price).setScale(8, RoundingMode.DOWN) * feeMultiplier).setScale(8, RoundingMode.DOWN)
    }
}
