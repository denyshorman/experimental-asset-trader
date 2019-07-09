package com.gitlab.dhorman.cryptotrader.trader.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonView
import com.gitlab.dhorman.cryptotrader.core.BareTrade
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.core.OrderSpeed
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.buyBaseAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.buyQuoteAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.sellBaseAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.core.sellQuoteAmount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.CurrencyType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import io.vavr.collection.Array
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
    val targetCurrency: Currency by lazy(LazyThreadSafetyMode.NONE) { market.currency(fromCurrencyType.inverse()) }

    @get:JsonView(Views.UI::class)
    val orderType: OrderType by lazy(LazyThreadSafetyMode.NONE) { market.orderType(fromCurrencyType.inverse()) }
}

data class TranIntentMarketCompleted(
    override val market: Market,
    override val orderSpeed: OrderSpeed,
    override val fromCurrencyType: CurrencyType,
    val trades: Array<BareTrade>
) : TranIntentMarket(market, orderSpeed, fromCurrencyType) {
    @get:JsonView(Views.UI::class)
    val fromAmount: Amount by lazy(LazyThreadSafetyMode.NONE) {
        var amount = BigDecimal.ZERO
        for (trade in trades) {
            amount += if (fromCurrencyType == CurrencyType.Base) {
                buyBaseAmount(trade.quoteAmount, trade.price)
            } else {
                if (trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
                    BigDecimal.ZERO
                } else {
                    sellQuoteAmount(trade.quoteAmount)
                }
            }
        }
        amount
    }

    @get:JsonView(Views.UI::class)
    val targetAmount: Amount by lazy(LazyThreadSafetyMode.NONE) {
        var amount = BigDecimal.ZERO
        for (trade in trades) {
            amount += if (fromCurrencyType == CurrencyType.Base) {
                buyQuoteAmount(trade.quoteAmount, trade.feeMultiplier)
            } else {
                if (trade.price.compareTo(BigDecimal.ZERO) == 0 && trade.feeMultiplier.compareTo(BigDecimal.ZERO) == 0) {
                    trade.quoteAmount
                } else {
                    sellBaseAmount(trade.quoteAmount, trade.price, trade.feeMultiplier)
                }
            }
        }
        amount
    }
}

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