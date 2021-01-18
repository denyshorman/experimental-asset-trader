package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader.core

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.core.PoloniexBuySellAmountCalculator
import org.springframework.stereotype.Component
import java.math.BigDecimal

@Component
class AdjustedPoloniexBuySellAmountCalculator(private val amountCalculator: PoloniexBuySellAmountCalculator) : AdjustedBuySellAmountCalculator {
    override fun quoteAmount(baseAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return amountCalculator.quoteAmount(baseAmount, price, feeMultiplier)
    }

    override fun fromAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return amountCalculator.fromAmountBuy(quoteAmount, price, feeMultiplier)
    }

    override fun targetAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return amountCalculator.targetAmountBuy(quoteAmount, price, feeMultiplier)
    }

    override fun fromAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return if (price.compareTo(BigDecimal.ZERO) == 0 && feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
            BigDecimal.ZERO
        } else {
            amountCalculator.fromAmountSell(quoteAmount, price, feeMultiplier)
        }
    }

    override fun targetAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
        return if (price.compareTo(BigDecimal.ZERO) == 0 && feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
            quoteAmount
        } else {
            amountCalculator.targetAmountSell(quoteAmount, price, feeMultiplier)
        }
    }
}
