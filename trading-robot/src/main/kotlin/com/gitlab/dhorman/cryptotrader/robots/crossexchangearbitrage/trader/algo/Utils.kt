package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.algo

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.OrderBook
import io.vavr.collection.SortedMap
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import java.math.BigDecimal
import java.math.RoundingMode

fun calculateBaseAmount(
    baseAssetPrecision: Int,
    quoteAmount: BigDecimal,
    price: BigDecimal,
    fee: BigDecimal,
): BigDecimal {
    val baseAmount = (quoteAmount * price).setScale(baseAssetPrecision, RoundingMode.HALF_EVEN)
    val baseAmountFee = (baseAmount * fee).setScale(baseAssetPrecision, RoundingMode.UP)
    return baseAmount - baseAmountFee
}

fun simulateMarketOrderPnl(
    baseAssetPrecision: Int,
    quoteAmount: BigDecimal,
    takerFee: BigDecimal,
    orderBook: SortedMap<BigDecimal, BigDecimal>,
): BigDecimal {
    var remainingQuoteAmount = quoteAmount
    var baseAmountSum = BigDecimal.ZERO

    for ((basePrice, quoteAmount0) in orderBook) {
        val quoteAmount1 = if (remainingQuoteAmount <= quoteAmount0) {
            val tmp = remainingQuoteAmount
            remainingQuoteAmount = BigDecimal.ZERO
            tmp
        } else {
            remainingQuoteAmount -= quoteAmount0
            quoteAmount0
        }

        val baseAmount = calculateBaseAmount(baseAssetPrecision, quoteAmount1, basePrice, takerFee)
        baseAmountSum += baseAmount

        if (remainingQuoteAmount.compareTo(BigDecimal.ZERO) == 0) {
            break
        }
    }

    return baseAmountSum
}

fun calculateFillAllMarketOrder(
    leftOrderBook: OrderBook,
    rightOrderBook: OrderBook,
    leftFee: BigDecimal,
    rightFee: BigDecimal,
    leftBaseAssetPrecision: Int,
    rightBaseAssetPrecision: Int,
    amount: BigDecimal,
): Pair<BigDecimal, BigDecimal> {
    val k0 = run {
        val a = simulateMarketOrderPnl(rightBaseAssetPrecision, amount, rightFee, rightOrderBook.bids)
        val b = simulateMarketOrderPnl(leftBaseAssetPrecision, amount, leftFee, leftOrderBook.asks)
        a - b
    }

    val k1 = run {
        val a = simulateMarketOrderPnl(leftBaseAssetPrecision, amount, leftFee, leftOrderBook.bids)
        val b = simulateMarketOrderPnl(rightBaseAssetPrecision, amount, rightFee, rightOrderBook.asks)
        a - b
    }

    return k0 to k1
}
