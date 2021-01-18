package com.gitlab.dhorman.cryptotrader.robots.binancearbitrage

import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.RoundingMode

@Component("BinanceBuySellAmountCalculator")
open class BuySellAmountCalculator {
    open fun quoteAmount(data: Data): BigDecimal {
        var q = data.amount.divide(data.price, data.baseAssetPrecision, RoundingMode.UP)
        val d = Data(q, data.price, data.feeMultiplier, data.baseAssetPrecision, data.quoteAssetPrecision)
        if (fromAmountBuy(d).compareTo(data.amount) != 0) {
            q = data.amount.divide(data.price, data.baseAssetPrecision, RoundingMode.DOWN)
        }
        return q
    }

    open fun fromAmountBuy(data: Data): BigDecimal {
        return (data.amount * data.price).setScale(data.quoteAssetPrecision, RoundingMode.UP)
    }

    open fun targetAmountBuy(data: Data): BigDecimal {
        return (data.amount * data.feeMultiplier).setScale(data.baseAssetPrecision, RoundingMode.DOWN)
    }

    open fun fromAmountSell(data: Data): BigDecimal {
        return data.amount
    }

    open fun targetAmountSell(data: Data): BigDecimal {
        val amount = (data.amount * data.price).setScale(data.quoteAssetPrecision, RoundingMode.DOWN)
        return (amount * data.feeMultiplier).setScale(data.quoteAssetPrecision, RoundingMode.DOWN)
    }

    data class Data(
        val amount: BigDecimal,
        val price: BigDecimal,
        val feeMultiplier: BigDecimal,
        val baseAssetPrecision: Int,
        val quoteAssetPrecision: Int
    )
}

fun BuySellAmountCalculator.baseAmountBuy(data: BuySellAmountCalculator.Data): BigDecimal {
    return fromAmountBuy(data)
}

fun BuySellAmountCalculator.quoteAmountBuy(data: BuySellAmountCalculator.Data): BigDecimal {
    return targetAmountBuy(data)
}

fun BuySellAmountCalculator.quoteAmountSell(data: BuySellAmountCalculator.Data): BigDecimal {
    return fromAmountSell(data)
}

fun BuySellAmountCalculator.baseAmountSell(data: BuySellAmountCalculator.Data): BigDecimal {
    return targetAmountSell(data)
}
