package com.gitlab.dhorman.cryptotrader.trader.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonView
import com.gitlab.dhorman.cryptotrader.core.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.CurrencyType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.trader.DataStreams
import com.gitlab.dhorman.cryptotrader.trader.core.AdjustedBuySellAmountCalculator
import io.vavr.collection.Array
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import kotlinx.coroutines.flow.first
import java.math.BigDecimal

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "tpe"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = TranIntentMarketCompleted::class, name = "c"),
    JsonSubTypes.Type(value = TranIntentMarketPartiallyCompleted::class, name = "pc"),
    JsonSubTypes.Type(value = TranIntentMarketPredicted::class, name = "p")
)
sealed class TranIntentMarket(
    open val market: Market,
    open val orderSpeed: OrderSpeed,
    open val fromCurrencyType: CurrencyType
) {
    @get:JsonView(Views.UI::class)
    val fromCurrency: Currency by lazy(LazyThreadSafetyMode.NONE) { market.currency(fromCurrencyType) }

    @get:JsonView(Views.UI::class)
    val targetCurrency: Currency by lazy(LazyThreadSafetyMode.NONE) { market.currency(!fromCurrencyType) }

    @get:JsonView(Views.UI::class)
    val orderType: OrderType by lazy(LazyThreadSafetyMode.NONE) { market.orderType(!fromCurrencyType) }
}

data class TranIntentMarketCompleted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType,
    val trades: Array<BareTrade>
) : TranIntentMarket(market, orderSpeed, fromCurrencyType)

data class TranIntentMarketPartiallyCompleted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType,
    val fromAmount: Amount
) : TranIntentMarket(market, orderSpeed, fromCurrencyType)

data class TranIntentMarketPredicted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType
) : TranIntentMarket(market, orderSpeed, fromCurrencyType)

object Views {
    open class DB
    open class UI : DB()
}


class TranIntentMarketExtensions(
    private val amountCalculator: AdjustedBuySellAmountCalculator,
    private val data: DataStreams
) {
    fun getInstantOrderTargetAmount(
        orderType: OrderType,
        fromAmount: Amount,
        takerFeeMultiplier: BigDecimal,
        orderBook: OrderBookAbstract
    ): Amount {
        var unusedFromAmount: Amount = fromAmount
        var toAmount = BigDecimal.ZERO

        if (orderType == OrderType.Buy) {
            if (orderBook.asks.length() == 0) return BigDecimal.ZERO

            for ((basePrice, quoteAmount) in orderBook.asks) {
                val availableFromAmount = amountCalculator.fromAmountBuy(quoteAmount, basePrice, takerFeeMultiplier)

                if (unusedFromAmount <= availableFromAmount) {
                    toAmount += amountCalculator.targetAmountBuy(amountCalculator.quoteAmount(unusedFromAmount, basePrice, takerFeeMultiplier), basePrice, takerFeeMultiplier)
                    break
                } else {
                    unusedFromAmount -= amountCalculator.fromAmountBuy(quoteAmount, basePrice, takerFeeMultiplier)
                    toAmount += amountCalculator.targetAmountBuy(quoteAmount, basePrice, takerFeeMultiplier)
                }
            }
        } else {
            if (orderBook.bids.length() == 0) return BigDecimal.ZERO

            for ((basePrice, quoteAmount) in orderBook.bids) {
                if (unusedFromAmount <= quoteAmount) {
                    toAmount += amountCalculator.targetAmountSell(unusedFromAmount, basePrice, takerFeeMultiplier)
                    break
                } else {
                    unusedFromAmount -= amountCalculator.fromAmountSell(quoteAmount, basePrice, takerFeeMultiplier)
                    toAmount += amountCalculator.targetAmountSell(quoteAmount, basePrice, takerFeeMultiplier)
                }
            }
        }

        return toAmount
    }

    fun getDelayedOrderTargetAmount(
        orderType: OrderType,
        fromAmount: Amount,
        makerFeeMultiplier: BigDecimal,
        orderBook: OrderBookAbstract
    ): Amount {
        if (orderType == OrderType.Buy) {
            if (orderBook.bids.length() == 0) return BigDecimal.ZERO

            val basePrice = orderBook.bids.head()._1
            val quoteAmount = amountCalculator.quoteAmount(fromAmount, basePrice, makerFeeMultiplier)

            return amountCalculator.targetAmountBuy(quoteAmount, basePrice, makerFeeMultiplier)
        } else {
            if (orderBook.asks.length() == 0) return BigDecimal.ZERO

            val basePrice = orderBook.asks.head()._1

            return amountCalculator.targetAmountSell(fromAmount, basePrice, makerFeeMultiplier)
        }
    }

    suspend fun fromAmount(market: TranIntentMarket, markets: Array<TranIntentMarket>, idx: Int): Amount {
        return when (market) {
            is TranIntentMarketCompleted -> fromAmount(market)
            is TranIntentMarketPartiallyCompleted -> market.fromAmount
            is TranIntentMarketPredicted -> predictedFromAmount(markets, idx)
        }
    }

    suspend fun targetAmount(market: TranIntentMarket, markets: Array<TranIntentMarket>, idx: Int): Amount {
        return when (market) {
            is TranIntentMarketCompleted -> targetAmount(market)
            is TranIntentMarketPartiallyCompleted -> predictedTargetAmount(market)
            is TranIntentMarketPredicted -> predictedTargetAmount(market, markets, idx)
        }
    }

    fun fromAmount(market: TranIntentMarketCompleted): Amount {
        var amount = BigDecimal.ZERO

        for (trade in market.trades) {
            amount += if (market.fromCurrencyType == CurrencyType.Base) {
                amountCalculator.fromAmountBuy(trade)
            } else {
                amountCalculator.fromAmountSell(trade)
            }
        }

        return amount
    }

    fun targetAmount(market: TranIntentMarketCompleted): Amount {
        var amount = BigDecimal.ZERO

        for (trade in market.trades) {
            amount += if (market.fromCurrencyType == CurrencyType.Base) {
                amountCalculator.targetAmountBuy(trade)
            } else {
                amountCalculator.targetAmountSell(trade)
            }
        }

        return amount
    }

    fun fromAmount(markets: Iterable<BareTrade>, orderType: OrderType): BigDecimal {
        return markets.asSequence()
            .map { amountCalculator.fromAmount(orderType, it) }
            .fold(BigDecimal.ZERO) { x, y -> x + y }
    }

    fun targetAmount(markets: Iterable<BareTrade>, orderType: OrderType): BigDecimal {
        return markets.asSequence()
            .map { amountCalculator.targetAmount(orderType, it) }
            .fold(BigDecimal.ZERO) { x, y -> x + y }
    }

    suspend fun predictedTargetAmount(market: TranIntentMarketPartiallyCompleted): Amount {
        val fee = data.fee.first()
        val orderBook = data.getOrderBookFlowBy(market.market).first()

        return if (market.orderSpeed == OrderSpeed.Instant) {
            getInstantOrderTargetAmount(market.orderType, market.fromAmount, fee.taker, orderBook)
        } else {
            getDelayedOrderTargetAmount(market.orderType, market.fromAmount, fee.maker, orderBook)
        }
    }

    suspend fun predictedTargetAmount(market: TranIntentMarketPredicted, markets: Array<TranIntentMarket>, idx: Int): Amount {
        val fee = data.fee.first()
        val orderBook = data.getOrderBookFlowBy(market.market).first()
        val fromAmount = predictedFromAmount(markets, idx)

        return if (market.orderSpeed == OrderSpeed.Instant) {
            getInstantOrderTargetAmount(market.orderType, fromAmount, fee.taker, orderBook)
        } else {
            getDelayedOrderTargetAmount(market.orderType, fromAmount, fee.maker, orderBook)
        }
    }

    suspend fun predictedFromAmount(markets: Array<TranIntentMarket>, idx: Int): Amount {
        val prevIdx = idx - 1
        return when (val prevTran = markets[prevIdx]) {
            is TranIntentMarketCompleted -> targetAmount(prevTran)
            is TranIntentMarketPartiallyCompleted -> predictedTargetAmount(prevTran)
            is TranIntentMarketPredicted -> predictedTargetAmount(prevTran, markets, prevIdx)
        }
    }

    fun partiallyCompletedMarketIndex(markets: Array<TranIntentMarket>): Int? {
        var i = 0
        for (market in markets) {
            if (market is TranIntentMarketPartiallyCompleted) break
            i++
        }
        return if (i == markets.length()) null else i
    }

    fun pathString(markets: Array<TranIntentMarket>): String {
        return markets.iterator()
            .map { "${it.market}${if (it.orderSpeed == OrderSpeed.Instant) "0" else "1"}" }
            .mkString("->")
    }

    fun id(markets: Array<TranIntentMarket>): String {
        return markets.iterator().map {
            val speed = if (it.orderSpeed == OrderSpeed.Instant) "0" else "1"
            "${it.market.baseCurrency}${it.market.quoteCurrency}$speed"
        }.mkString("")
    }
}
