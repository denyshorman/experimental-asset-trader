package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model

enum class CurrencyType {
    Base,
    Quote;

    operator fun not() = when (this) {
        Base -> Quote
        Quote -> Base
    }
}
