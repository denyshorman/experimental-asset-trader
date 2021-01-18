package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.core

import com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core.BuySellAmountCalculator
import java.math.BigDecimal

interface PoloniexBuySellAmountCalculator : BuySellAmountCalculator

fun PoloniexBuySellAmountCalculator.quoteAmount(baseAmount: BigDecimal, price: BigDecimal): BigDecimal {
    return quoteAmount(baseAmount, price, BigDecimal.ZERO)
}

fun PoloniexBuySellAmountCalculator.fromAmountBuy(quoteAmount: BigDecimal, price: BigDecimal): BigDecimal {
    return fromAmountBuy(quoteAmount, price, BigDecimal.ZERO)
}

fun PoloniexBuySellAmountCalculator.targetAmountBuy(quoteAmount: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
    return targetAmountBuy(quoteAmount, BigDecimal.ZERO, feeMultiplier)
}

fun PoloniexBuySellAmountCalculator.fromAmountSell(quoteAmount: BigDecimal): BigDecimal {
    return fromAmountSell(quoteAmount, BigDecimal.ZERO, BigDecimal.ZERO)
}
