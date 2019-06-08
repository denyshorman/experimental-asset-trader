package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.service.poloniex.core.buyBaseAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.calcQuoteAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import io.vavr.collection.List
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

typealias InstantDelayedOrderChain = List<InstantDelayedOrder>

data class ExhaustivePath(
    val targetPath: TargetPath,
    val chain: InstantDelayedOrderChain
) {
    val id: String by lazy {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> "${it.market}0"
                is DelayedOrder -> "${it.market}1"
            }
        }.mkString("")
    }

    val simpleMultiplier: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> it.orderMultiplierSimple
                is DelayedOrder -> it.orderMultiplier
            }
        }.reduceLeft { a, b -> a * b }
    }

    val amountMultiplier: BigDecimal by lazy(LazyThreadSafetyMode.NONE) {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> it.orderMultiplierAmount
                is DelayedOrder -> it.orderMultiplier
            }
        }.reduceLeft { a, b -> a * b }
    }

    val avgWaitTime: Long by lazy(LazyThreadSafetyMode.NONE) {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> 0
                is DelayedOrder -> it.stat.ttwAvgMs
            }
        }.reduceLeft { a, b -> a + b }
    }

    val maxWaitTime: Long by lazy(LazyThreadSafetyMode.NONE) {
        chain.iterator().map {
            when (it) {
                is InstantOrder -> 0
                is DelayedOrder -> it.stat.ttwAvgMs + it.stat.ttwStdDev
            }
        }.reduceLeft { a, b -> a + b }
    }

    val recommendedStartAmount: Amount by lazy(LazyThreadSafetyMode.NONE) {
        var startAmount: BigDecimal? = null
        var targetCurrency = targetPath._2

        for (order in chain.reverseIterator()) {
            if (order is DelayedOrder) {
                val avgAmount = if (order.stat.avgAmount.compareTo(BigDecimal.ZERO) == 0) {
                    MaxAvg
                } else {
                    order.stat.avgAmount
                }

                startAmount = if (order.market.quoteCurrency == targetCurrency) {
                    if (startAmount == null) {
                        buyBaseAmount(avgAmount, order.basePrice)
                    } else {
                        val quoteAmount = startAmount.divide(order.feeMultiplier, 8, RoundingMode.DOWN)
                        buyBaseAmount(avgAmount.min(quoteAmount), order.basePrice)
                    }
                } else {
                    if (startAmount == null) {
                        avgAmount
                    } else {
                        calcQuoteAmount(startAmount, order.basePrice).min(avgAmount)
                    }
                }
            }

            targetCurrency = order.market.other(targetCurrency)!!
        }

        startAmount ?: chain.head().fromAmount
    }

    val simpleRisk: Int by lazy(LazyThreadSafetyMode.NONE) {
        var bit = 0
        var orderType = 0
        chain.reverseIterator().forEach { order ->
            when (order) {
                is InstantOrder -> run {}
                is DelayedOrder -> orderType = orderType or (1 shl bit)
            }
            bit++
        }

        sr[chain.length() - 1][orderType]
    }

    val lastTran: Long by lazy(LazyThreadSafetyMode.NONE) {
        val now = Instant.now()
        val oldTranTime = chain.iterator().map {
            when (it) {
                is InstantOrder -> now
                is DelayedOrder -> it.stat.lastTranTs
            }
        }.min().get()

        now.toEpochMilli() - oldTranTime.toEpochMilli()
    }

    val waitTime: Long by lazy(LazyThreadSafetyMode.NONE) {
        val avgTime0 = avgWaitTime + maxWaitTime
        val fromAmount = chain.head().fromAmount
        val deltaAmount = recommendedStartAmount - fromAmount
        val deltaTime = if (deltaAmount >= BigDecimal.ZERO) {
            0L
        } else {
            (deltaAmount.abs() * avgTime0.toBigDecimal()).divide(recommendedStartAmount, 0, RoundingMode.DOWN).toLong()
        }

        avgTime0 + deltaTime
    }

    override fun hashCode(): Int = id.hashCode()

    override fun equals(other: Any?): Boolean = when (other) {
        is ExhaustivePath -> id == other.id
        else -> false
    }

    companion object {
        private val sr1 = arrayOf(0, 0)
        private val sr2 = arrayOf(0, 100, 0, 200)
        private val sr3 = arrayOf(0, 100, 110, 200, 0, 210, 210, 300)
        private val sr4 = arrayOf(0, 100, 110, 200, 120, 210, 210, 300, 0, 240, 240, 310, 220, 310, 310, 400)
        private val sr = arrayOf(sr1, sr2, sr3, sr4)
        private val MaxAvg = Double.MAX_VALUE.toBigDecimal()
    }
}