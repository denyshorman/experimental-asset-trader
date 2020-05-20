package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.core.BareTrade
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.ExtendedPoloniexApi
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Price
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.TradeCategory
import io.vavr.collection.List
import io.vavr.kotlin.toVavrStream
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import mu.KotlinLogging
import java.time.Instant

private val logger = KotlinLogging.logger {}

suspend fun ExtendedPoloniexApi.missedTrades(orderId: Long, processedTrades: Set<Long>): List<BareTrade> {
    while (true) {
        try {
            return this.orderTrades(orderId).toVavrStream()
                .filter { !processedTrades.contains(it.tradeId) }
                .map { trade -> BareTrade(trade.amount, trade.price, trade.feeMultiplier) }
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
    processedTrades: Set<Long>
): List<BareTrade> {
    while (true) {
        try {
            return this.tradeHistory(market, fromTs, limit = 2000).toVavrStream()
                .flatMap { it._2 }
                .filter {
                    it.type == orderType
                        && it.category == tradeCategory
                        && it.price == price
                        && !processedTrades.contains(it.tradeId)
                }
                .map { trade -> BareTrade(trade.amount, trade.price, trade.feeMultiplier) }
                .toList()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logger.warn("Can't get trade history: ${e.message}")
            delay(1000)
        }
    }
}
