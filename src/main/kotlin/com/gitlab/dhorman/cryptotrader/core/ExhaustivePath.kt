package com.gitlab.dhorman.cryptotrader.core

import io.vavr.collection.List
import java.math.BigDecimal
import java.math.RoundingMode

typealias InstantDelayedOrderChain = List<InstantDelayedOrder>

data class ExhaustivePath(
    val targetPath: TargetPath,
    val chain: InstantDelayedOrderChain
) {
    val id: String by lazy(LazyThreadSafetyMode.NONE) {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> "${it.market.baseCurrency}${it.market.quoteCurrency}0"
                is DelayedOrder -> "${it.market.baseCurrency}${it.market.quoteCurrency}1"
            }
        }.mkString("")
    }

    val delta: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
        val fromAmount = chain.head().fromAmount
        val toAmount = chain.last().toAmount
        toAmount - fromAmount
    }

    val profitability: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
        val fromAmount = chain.head().fromAmount
        val toAmount = chain.last().toAmount
        val deltaAmount = toAmount - fromAmount

        amountPerDay * deltaAmount
    }

    val amountPerDay: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
        var amount: BigDecimal? = null
        var targetCurrency = targetPath._2

        for (order in chain.reverseIterator()) {
            targetCurrency = order.market.other(targetCurrency)!!

            amount = when (order) {
                is DelayedOrder -> run {
                    if (amount == null) {
                        if (targetCurrency == order.market.baseCurrency) {
                            order.stat.baseQuoteAvgAmount._1
                        } else {
                            order.stat.baseQuoteAvgAmount._2
                        }
                    } else {
                        if (targetCurrency == order.market.baseCurrency) {
                            (amount!!.min(order.stat.baseQuoteAvgAmount._2) * order.stat.baseQuoteAvgAmount._1).divide(
                                order.stat.baseQuoteAvgAmount._2,
                                16,
                                RoundingMode.DOWN
                            )
                        } else {
                            (amount!!.min(order.stat.baseQuoteAvgAmount._1) * order.stat.baseQuoteAvgAmount._2).divide(
                                order.stat.baseQuoteAvgAmount._1,
                                16,
                                RoundingMode.DOWN
                            )
                        }
                    }
                }
                is InstantOrder -> run {
                    if (amount != null) {
                        if (targetCurrency == order.market.baseCurrency) {
                            val price =
                                (order.fromAmount * order.feeMultiplier).divide(order.toAmount, 16, RoundingMode.DOWN)
                            amount!! * price
                        } else {
                            val price =
                                order.toAmount.divide(order.fromAmount * order.feeMultiplier, 8, RoundingMode.DOWN)
                            amount!!.divide(price, 16, RoundingMode.DOWN)
                        }
                    } else {
                        null
                    }
                }
            }
        }

        amount ?: BigDecimalMax
    }

    fun longPathString(): String {
        return this.chain.iterator().map {
            when (it) {
                is InstantOrder -> "${it.market}0"
                is DelayedOrder -> "${it.market}1"
            }
        }.mkString("->")
    }

    override fun hashCode(): Int = id.hashCode()

    override fun equals(other: Any?): Boolean = when (other) {
        is ExhaustivePath -> id == other.id
        else -> false
    }

    companion object {
        private val BigDecimalMax = BigDecimal("999999999999")
    }
}