package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core

import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.SubOrderBook
import io.vavr.collection.TreeMap
import java.math.BigDecimal


abstract class OrderBookAbstract(
    open val asks: SubOrderBook = TreeMap.empty(),
    open val bids: SubOrderBook = TreeMap.empty(compareByDescending { it })
)

data class FeeMultiplier(val maker: BigDecimal, val taker: BigDecimal)

enum class OrderSpeed {
    Instant,
    Delayed
}

open class BareTrade(
    open val quoteAmount: BigDecimal,
    open val price: BigDecimal,
    open val feeMultiplier: BigDecimal
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BareTrade

        if (quoteAmount != other.quoteAmount) return false
        if (price != other.price) return false
        if (feeMultiplier != other.feeMultiplier) return false

        return true
    }

    override fun hashCode(): Int {
        var result = quoteAmount.hashCode()
        result = 31 * result + price.hashCode()
        result = 31 * result + feeMultiplier.hashCode()
        return result
    }

    override fun toString(): String {
        return "BareTrade(quoteAmount=$quoteAmount, price=$price, feeMultiplier=$feeMultiplier)"
    }
}
