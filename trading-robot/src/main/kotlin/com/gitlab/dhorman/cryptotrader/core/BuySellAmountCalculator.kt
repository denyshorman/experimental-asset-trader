package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import java.math.BigDecimal

interface BuySellAmountCalculator {
    fun quoteAmount(baseAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal
    fun fromAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal
    fun targetAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal
    fun fromAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal
    fun targetAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal
}

enum class AmountType {
    From,
    Target;

    operator fun not() = when (this) {
        From -> Target
        Target -> From
    }
}

fun BuySellAmountCalculator.baseAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
    return fromAmountBuy(quoteAmount, price, feeMultiplier)
}

fun BuySellAmountCalculator.quoteAmountBuy(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
    return targetAmountBuy(quoteAmount, price, feeMultiplier)
}

fun BuySellAmountCalculator.quoteAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
    return fromAmountSell(quoteAmount, price, feeMultiplier)
}

fun BuySellAmountCalculator.baseAmountSell(quoteAmount: BigDecimal, price: BigDecimal, feeMultiplier: BigDecimal): BigDecimal {
    return targetAmountSell(quoteAmount, price, feeMultiplier)
}

fun BuySellAmountCalculator.baseAmountBuy(trade: BareTrade): BigDecimal {
    return fromAmountBuy(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.quoteAmountBuy(trade: BareTrade): BigDecimal {
    return targetAmountBuy(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.quoteAmountSell(trade: BareTrade): BigDecimal {
    return fromAmountSell(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.baseAmountSell(trade: BareTrade): BigDecimal {
    return targetAmountSell(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.baseAmount(orderType: OrderType, trade: BareTrade): BigDecimal {
    return when (orderType) {
        OrderType.Buy -> baseAmountBuy(trade)
        OrderType.Sell -> baseAmountSell(trade)
    }
}

fun BuySellAmountCalculator.quoteAmount(orderType: OrderType, trade: BareTrade): BigDecimal {
    return when (orderType) {
        OrderType.Buy -> quoteAmountBuy(trade)
        OrderType.Sell -> quoteAmountSell(trade)
    }
}

fun BuySellAmountCalculator.fromAmountBuy(trade: BareTrade): BigDecimal {
    return fromAmountBuy(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.targetAmountBuy(trade: BareTrade): BigDecimal {
    return targetAmountBuy(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.fromAmountSell(trade: BareTrade): BigDecimal {
    return fromAmountSell(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.targetAmountSell(trade: BareTrade): BigDecimal {
    return targetAmountSell(trade.quoteAmount, trade.price, trade.feeMultiplier)
}

fun BuySellAmountCalculator.fromAmount(orderType: OrderType, trade: BareTrade): BigDecimal {
    return when (orderType) {
        OrderType.Buy -> fromAmountBuy(trade)
        OrderType.Sell -> fromAmountSell(trade)
    }
}

fun BuySellAmountCalculator.targetAmount(orderType: OrderType, trade: BareTrade): BigDecimal {
    return when (orderType) {
        OrderType.Buy -> targetAmountBuy(trade)
        OrderType.Sell -> targetAmountSell(trade)
    }
}

fun BuySellAmountCalculator.amount(amountType: AmountType, orderType: OrderType, trade: BareTrade): BigDecimal {
    return when (amountType) {
        AmountType.From -> fromAmount(orderType, trade)
        AmountType.Target -> targetAmount(orderType, trade)
    }
}
