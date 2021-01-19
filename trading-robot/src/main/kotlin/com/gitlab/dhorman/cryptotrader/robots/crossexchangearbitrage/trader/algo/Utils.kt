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
): Pair<BigDecimal, BigDecimal> {
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

    return baseAmountSum to remainingQuoteAmount
}

fun calculateFillAllMarketOrder(
    leftOrderBook: OrderBook,
    rightOrderBook: OrderBook,
    leftFee: BigDecimal,
    rightFee: BigDecimal,
    leftBaseAssetPrecision: Int,
    rightBaseAssetPrecision: Int,
    amount: BigDecimal,
): Pair<BigDecimal?, BigDecimal?> {
    val k0 = run {
        val (b0, q0) = simulateMarketOrderPnl(rightBaseAssetPrecision, amount, rightFee, rightOrderBook.bids)
        val (b1, q1) = simulateMarketOrderPnl(leftBaseAssetPrecision, amount, leftFee, leftOrderBook.asks)

        if (q0.compareTo(BigDecimal.ZERO) == 0 && q1.compareTo(BigDecimal.ZERO) == 0) {
            b0 - b1
        } else {
            null
        }
    }

    val k1 = run {
        val (b0, q0) = simulateMarketOrderPnl(leftBaseAssetPrecision, amount, leftFee, leftOrderBook.bids)
        val (b1, q1) = simulateMarketOrderPnl(rightBaseAssetPrecision, amount, rightFee, rightOrderBook.asks)

        if (q0.compareTo(BigDecimal.ZERO) == 0 && q1.compareTo(BigDecimal.ZERO) == 0) {
            b0 - b1
        } else {
            null
        }
    }

    return k0 to k1
}
