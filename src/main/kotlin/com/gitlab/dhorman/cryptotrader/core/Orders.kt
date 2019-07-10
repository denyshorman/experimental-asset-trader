package com.gitlab.dhorman.cryptotrader.core

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.*
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

        var trades: List<BareTrade> = List.empty()
        var unusedFromCurrencyAmount: Amount = initCurrencyAmount
        var targetCurrencyAmount = BigDecimal.ZERO
        val orderMultiplierSimple: BigDecimal
        val orderMultiplierAmount: BigDecimal

        if (orderTpe == OrderType.Buy) {
            if (orderBook.asks.length() == 0) return null

            for ((basePrice, quoteAmount) in orderBook.asks) {
                val availableFromAmount = buyBaseAmount(quoteAmount, basePrice)

                if (unusedFromCurrencyAmount <= availableFromAmount) {
                    val tradeQuoteAmount = calcQuoteAmount(unusedFromCurrencyAmount, basePrice)
                    targetCurrencyAmount += buyQuoteAmount(tradeQuoteAmount, takerFeeMultiplier)
                    trades = trades.prepend(BareTrade(tradeQuoteAmount, basePrice, takerFeeMultiplier))
                    unusedFromCurrencyAmount = BigDecimal.ZERO
                    break
                } else {
                    unusedFromCurrencyAmount -= availableFromAmount
                    targetCurrencyAmount += buyQuoteAmount(quoteAmount, takerFeeMultiplier)
                    trades = trades.prepend(BareTrade(quoteAmount, basePrice, takerFeeMultiplier))
                }
            }

            val price = orderBook.asks.head()._1
            orderMultiplierSimple = takerFeeMultiplier.divide(price, 8, RoundingMode.DOWN)
        } else {
            if (orderBook.bids.length() == 0) return null

            for ((basePrice, quoteAmount) in orderBook.bids) {
                if (unusedFromCurrencyAmount <= quoteAmount) {
                    targetCurrencyAmount += sellBaseAmount(unusedFromCurrencyAmount, basePrice, takerFeeMultiplier)
                    trades = trades.prepend(BareTrade(unusedFromCurrencyAmount, basePrice, takerFeeMultiplier))
                    unusedFromCurrencyAmount = BigDecimal.ZERO
                    break
                } else {
                    unusedFromCurrencyAmount -= sellQuoteAmount(quoteAmount)
                    targetCurrencyAmount += sellBaseAmount(quoteAmount, basePrice, takerFeeMultiplier)
                    trades = trades.prepend(BareTrade(quoteAmount, basePrice, takerFeeMultiplier))
                }
            }

            val price = orderBook.bids.head()._1
            orderMultiplierSimple = (price * takerFeeMultiplier).setScale(8, RoundingMode.DOWN)
        }

        orderMultiplierAmount = if (initCurrencyAmount.compareTo(BigDecimal.ZERO) != 0) {
            targetCurrencyAmount.divide(initCurrencyAmount, 8, RoundingMode.DOWN)
        } else {
            BigDecimal.ONE
        }

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

            quoteAmount = calcQuoteAmount(fromAmount, basePrice)
            toAmount = buyQuoteAmount(quoteAmount, makerFeeMultiplier)
        } else {
            if (orderBook.asks.length() == 0) return null

            basePrice = orderBook.asks.head()._1.cut8minus1

            if (orderBook.bids.length() > 0 && basePrice.compareTo(orderBook.bids.head()._1) == 0) return null

            quoteAmount = fromAmount
            toAmount = sellBaseAmount(quoteAmount, basePrice, makerFeeMultiplier)
        }

        orderMultiplier = if (fromAmount.compareTo(BigDecimal.ZERO) != 0) {
            toAmount.divide(fromAmount, 8, RoundingMode.DOWN)
        } else {
            BigDecimal.ONE
        }

        return DelayedOrder(
            market,
            fromCurrency,
            targetCurrency,
            fromAmount,
            basePrice,
            quoteAmount,
            makerFeeMultiplier,
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
sealed class InstantDelayedOrder(
    open val market: Market,
    open val fromCurrency: Currency,
    open val targetCurrency: Currency,
    open val fromAmount: Amount,
    open val toAmount: Amount,
    open val orderType: OrderType,
    open val feeMultiplier: BigDecimal
)

data class InstantOrder(
    override val market: Market,
    override val fromCurrency: Currency,
    override val targetCurrency: Currency,
    override val fromAmount: Amount,
    override val toAmount: Amount,
    override val orderType: OrderType,
    val orderMultiplierSimple: BigDecimal,
    val orderMultiplierAmount: BigDecimal,
    val unusedFromCurrencyAmount: Amount,
    override val feeMultiplier: BigDecimal,
    val trades: List<BareTrade>
) : InstantDelayedOrder(market, fromCurrency, targetCurrency, fromAmount, toAmount, orderType, feeMultiplier)

data class DelayedOrder(
    override val market: Market,
    override val fromCurrency: Currency,
    override val targetCurrency: Currency,
    override val fromAmount: Amount,
    val basePrice: Price,
    val quoteAmount: Amount,
    override val feeMultiplier: BigDecimal,
    override val toAmount: Amount,
    override val orderType: OrderType,
    val orderMultiplier: BigDecimal,
    val stat: TradeStatOrder
) : InstantDelayedOrder(market, fromCurrency, targetCurrency, fromAmount, toAmount, orderType, feeMultiplier)