package com.gitlab.dhorman.cryptotrader.core

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Price
import io.vavr.collection.List
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import java.math.BigDecimal
import java.math.RoundingMode

object Orders {
    fun getInstantOrder(
        market: Market,
        targetCurrency: Currency,
        initCurrencyAmount: Amount,
        takerFeeMultiplier: BigDecimal,
        orderBook: OrderBookAbstract
    ): InstantOrder? {
        val fromCurrency = market.other(targetCurrency) ?: return null
        val orderTpe = market.orderType(targetCurrency) ?: return null

        var trades: List<InstantOrder.Companion.Trade> = List.empty()
        var unusedFromCurrencyAmount: Amount = initCurrencyAmount
        var targetCurrencyAmount = BigDecimal.ZERO
        val orderMultiplierSimple: BigDecimal
        val orderMultiplierAmount: BigDecimal

        if (orderTpe == OrderType.Buy) {
            if (orderBook.asks.length() == 0) return null

            for ((basePrice, quoteAmount) in orderBook.asks) {
                val availableAmount = quoteAmount * basePrice

                if (unusedFromCurrencyAmount <= availableAmount) {
                    targetCurrencyAmount += unusedFromCurrencyAmount.setScale(12, RoundingMode.HALF_EVEN) / basePrice
                    val tradeAmount = unusedFromCurrencyAmount.setScale(12, RoundingMode.HALF_EVEN) / basePrice
                    trades = trades.prepend(InstantOrder.Companion.Trade(basePrice, tradeAmount))
                    unusedFromCurrencyAmount = BigDecimal.ZERO
                    break
                } else {
                    unusedFromCurrencyAmount -= availableAmount
                    targetCurrencyAmount += availableAmount.setScale(12, RoundingMode.HALF_EVEN) / basePrice
                    trades = trades.prepend(InstantOrder.Companion.Trade(basePrice, quoteAmount))
                }
            }

            val price = orderBook.asks.head()._1
            orderMultiplierSimple = (BigDecimal.ONE.setScale(12, RoundingMode.HALF_EVEN) / price) * takerFeeMultiplier
        } else {
            if (orderBook.bids.length() == 0) return null

            for ((basePrice, quoteAmount) in orderBook.bids) {
                if (unusedFromCurrencyAmount <= quoteAmount) {
                    targetCurrencyAmount += unusedFromCurrencyAmount * basePrice
                    trades = trades.prepend(InstantOrder.Companion.Trade(basePrice, unusedFromCurrencyAmount))
                    unusedFromCurrencyAmount = BigDecimal.ZERO
                    break
                } else {
                    unusedFromCurrencyAmount -= quoteAmount
                    targetCurrencyAmount += quoteAmount * basePrice
                    trades = trades.prepend(InstantOrder.Companion.Trade(basePrice, quoteAmount))
                }
            }

            val price = orderBook.bids.head()._1
            orderMultiplierSimple = price * takerFeeMultiplier
        }

        targetCurrencyAmount *= takerFeeMultiplier
        orderMultiplierAmount = targetCurrencyAmount.setScale(12, RoundingMode.HALF_EVEN) / initCurrencyAmount

        return InstantOrder(
            market,
            fromCurrency,
            targetCurrency,
            initCurrencyAmount,
            targetCurrencyAmount,
            orderTpe,
            orderMultiplierSimple,
            orderMultiplierAmount,
            unusedFromCurrencyAmount,
            takerFeeMultiplier,
            trades
        )
    }

    fun getDelayedOrder(
        market: Market,
        targetCurrency: Currency,
        fromAmount: Amount,
        makerFeeMultiplier: BigDecimal,
        orderBook: OrderBookAbstract,
        stat: TradeStatOrder
    ): DelayedOrder? {
        val fromCurrency = market.other(targetCurrency) ?: return null
        val orderTpe = market.orderType(targetCurrency) ?: return null

        val basePrice: Price
        val quoteAmount: Amount
        val toAmount: Amount
        val orderMultiplier: BigDecimal

        if (orderTpe == OrderType.Buy) {
            if (orderBook.bids.length() == 0) return null

            basePrice = orderBook.bids.head()._1.cut8add1

            if (orderBook.asks.length() > 0 && basePrice.compareTo(orderBook.asks.head()._1) == 0) return null

            quoteAmount = fromAmount.setScale(12, RoundingMode.HALF_EVEN) / basePrice
            toAmount = quoteAmount * makerFeeMultiplier
            orderMultiplier = makerFeeMultiplier.setScale(12, RoundingMode.HALF_EVEN) / basePrice
        } else {
            if (orderBook.asks.length() == 0) return null

            basePrice = orderBook.asks.head()._1.cut8minus1

            if (orderBook.bids.length() > 0 && basePrice.compareTo(orderBook.bids.head()._1) == 0) return null

            quoteAmount = fromAmount
            toAmount = quoteAmount * basePrice * makerFeeMultiplier
            orderMultiplier = makerFeeMultiplier * basePrice
        }

        return DelayedOrder(
            market,
            fromCurrency,
            targetCurrency,
            fromAmount,
            basePrice,
            quoteAmount,
            toAmount,
            orderTpe,
            orderMultiplier,
            stat
        )
    }

    fun getDelayedOrderReverse(
        market: Market,
        targetCurrency: Currency,
        toAmount: Amount,
        makerFeeMultiplier: BigDecimal,
        orderBook: OrderBookAbstract,
        stat: TradeStatOrder
    ): DelayedOrder? {
        val fromCurrency = market.other(targetCurrency) ?: return null
        val orderTpe = market.orderType(targetCurrency) ?: return null

        val basePrice: Price
        val quoteAmount: Amount
        val fromAmount: Amount
        val orderMultiplier: BigDecimal

        if (orderTpe == OrderType.Buy) {
            if (orderBook.bids.length() == 0) return null

            basePrice = orderBook.bids.head()._1.cut8add1

            if (orderBook.asks.length() > 0 && basePrice.compareTo(orderBook.asks.head()._1) == 0) return null

            quoteAmount = toAmount.setScale(12, RoundingMode.HALF_EVEN) / makerFeeMultiplier
            fromAmount = quoteAmount * basePrice
            orderMultiplier = makerFeeMultiplier.setScale(12, RoundingMode.HALF_EVEN) / basePrice
        } else {
            if (orderBook.asks.length() == 0) return null

            basePrice = orderBook.asks.head()._1.cut8minus1

            if (orderBook.bids.length() > 0 && basePrice.compareTo(orderBook.bids.head()._1) == 0) return null

            quoteAmount = toAmount.setScale(12, RoundingMode.HALF_EVEN) / (basePrice * makerFeeMultiplier)
            fromAmount = quoteAmount
            orderMultiplier = makerFeeMultiplier * basePrice
        }

        return DelayedOrder(
            market,
            fromCurrency,
            targetCurrency,
            fromAmount,
            basePrice,
            quoteAmount,
            toAmount,
            orderTpe,
            orderMultiplier,
            stat
        )
    }
}


@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.WRAPPER_OBJECT
)
@JsonSubTypes(
    JsonSubTypes.Type(value = InstantOrder::class, name = "InstantOrder"),
    JsonSubTypes.Type(value = DelayedOrder::class, name = "DelayedOrder")
)
sealed class InstantDelayedOrder

data class InstantOrder(
    val market: Market,
    val fromCurrency: Currency,
    val targetCurrency: Currency,
    val fromAmount: Amount,
    val toAmount: Amount,
    val orderType: OrderType,
    val orderMultiplierSimple: BigDecimal,
    val orderMultiplierAmount: BigDecimal,
    val unusedFromCurrencyAmount: Amount,
    val feeMultiplier: BigDecimal,
    val trades: List<InstantOrder.Companion.Trade>
) : InstantDelayedOrder() {
    companion object {
        data class Trade(
            val price: Price,
            val amount: Amount
        )
    }
}

data class DelayedOrder(
    val market: Market,
    val fromCurrency: Currency,
    val targetCurrency: Currency,
    val fromAmount: Amount,
    val basePrice: Price,
    val quoteAmount: Amount,
    val toAmount: Amount,
    val orderType: OrderType,
    val orderMultiplier: BigDecimal,
    val stat: TradeStatOrder
) : InstantDelayedOrder()