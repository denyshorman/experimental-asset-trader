package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.SubOrderBook
import io.vavr.collection.TreeMap
import java.math.BigDecimal
import java.time.Instant


abstract class OrderBookAbstract(
    open val asks: SubOrderBook = TreeMap.empty(),
    open val bids: SubOrderBook = TreeMap.empty(compareByDescending { it })
)

data class OrderBook(
    override val asks: SubOrderBook = TreeMap.empty(),
    override val bids: SubOrderBook = TreeMap.empty(compareByDescending { it })
) : OrderBookAbstract(asks, bids)

data class FeeMultiplier(val maker: BigDecimal, val taker: BigDecimal)

data class TradeStat(
    val sell: TradeStatOrder,
    val buy: TradeStatOrder
)

data class TradeStatOrder(
    val ttwAvgMs: Long,
    val ttwVariance: Long,
    val ttwStdDev: Long,
    val minAmount: BigDecimal,
    val maxAmount: BigDecimal,
    val avgAmount: BigDecimal,
    val varianceAmount: BigDecimal,
    val stdDevAmount: BigDecimal,
    val firstTranTs: Instant,
    val lastTranTs: Instant
)

enum class OrderSpeed {
    Instant,
    Delayed
}

data class BareTrade(
    val quoteAmount: BigDecimal,
    val price: BigDecimal,
    val feeMultiplier: BigDecimal
)