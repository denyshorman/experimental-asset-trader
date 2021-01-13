package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model

data class Market(val baseCurrency: Currency, val quoteCurrency: Currency) {
    val b get() = baseCurrency
    val q get() = quoteCurrency

    val symbol = "$baseCurrency$quoteCurrency"

    fun tpe(currency: Currency): CurrencyType? {
        if (currency == baseCurrency) return CurrencyType.Base
        if (currency == quoteCurrency) return CurrencyType.Quote
        return null
    }

    fun currency(tpe: CurrencyType): Currency {
        return when (tpe) {
            CurrencyType.Base -> baseCurrency
            CurrencyType.Quote -> quoteCurrency
        }
    }

    fun other(currency: Currency): Currency? {
        if (currency == baseCurrency) return quoteCurrency
        if (currency == quoteCurrency) return baseCurrency
        return null
    }

    fun orderType(targetCurrencyType: CurrencyType): OrderType {
        return when (targetCurrencyType) {
            CurrencyType.Base -> OrderType.Sell
            CurrencyType.Quote -> OrderType.Buy
        }
    }

    fun orderType(currencyType: AmountType, currency: Currency): OrderType? {
        return when (currencyType) {
            AmountType.From -> when (currency) {
                baseCurrency -> OrderType.Buy
                quoteCurrency -> OrderType.Sell
                else -> return null
            }
            AmountType.Target -> when (currency) {
                baseCurrency -> OrderType.Sell
                quoteCurrency -> OrderType.Buy
                else -> return null
            }
        }
    }

    fun fromCurrency(orderType: OrderType): Currency {
        return when (orderType) {
            OrderType.Sell -> quoteCurrency
            OrderType.Buy -> baseCurrency
        }
    }

    fun targetCurrency(orderType: OrderType): Currency {
        return when (orderType) {
            OrderType.Sell -> baseCurrency
            OrderType.Buy -> quoteCurrency
        }
    }

    fun contains(currency: Currency): Boolean {
        return currency == baseCurrency || currency == quoteCurrency
    }

    fun find(currencies: Iterable<Currency>): Currency? {
        return currencies.find { currency -> currency == baseCurrency || currency == quoteCurrency }
    }

    override fun toString() = symbol
}
