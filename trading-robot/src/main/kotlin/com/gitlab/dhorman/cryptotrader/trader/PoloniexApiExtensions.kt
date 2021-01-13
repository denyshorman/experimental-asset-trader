package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.BareTrade
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import mu.KotlinLogging
import java.math.BigDecimal
import java.time.Instant

private val logger = KotlinLogging.logger {}

suspend fun ExtendedPoloniexApi.missedTrades(orderId: Long, processedTrades: Set<Long>): List<PoloniexTrade> {
    while (true) {
        try {
            return this.orderTrades(orderId).asSequence()
                .filter { !processedTrades.contains(it.tradeId) }
                .map { it.toPoloniexTrade(orderId) }
                .toList()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logger.warn("Can't get trades for order id $orderId: ${e.message}")
            delay(1000)
        }
    }
}

suspend fun ExtendedPoloniexApi.missedTrades(
    market: Market,
    orderType: OrderType,
    tradeCategory: TradeCategory,
    fromTs: Instant,
    price: Price,
    priceUpperLimit: Boolean,
    processedTrades: Set<Long>
): List<PoloniexTrade> {
    while (true) {
        try {
            return this.tradeHistory(market, fromTs, limit = 2000).asSequence()
                .flatMap { it._2.asSequence() }
                .filter {
                    it.type == orderType
                        && it.category == tradeCategory
                        && (if (priceUpperLimit) if (orderType == OrderType.Buy) it.price >= price else it.price <= price else it.price.compareTo(price) == 0)
                        && !processedTrades.contains(it.tradeId)
                }
                .map { it.toPoloniexTrade() }
                .toList()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logger.warn("Can't get trade history: ${e.message}")
            delay(1000)
        }
    }
}

data class PoloniexTrade(
    val orderId: Long,
    val tradeId: Long,
    val type: OrderType,
    override val quoteAmount: BigDecimal,
    override val price: BigDecimal,
    override val feeMultiplier: BigDecimal
) : BareTrade(quoteAmount, price, feeMultiplier)


fun OrderTrade.toPoloniexTrade(orderId: Long): PoloniexTrade {
    return PoloniexTrade(orderId, tradeId, type, amount, price, feeMultiplier)
}

fun TradeHistoryPrivate.toPoloniexTrade(): PoloniexTrade {
    return PoloniexTrade(orderId, tradeId, type, amount, price, feeMultiplier)
}
