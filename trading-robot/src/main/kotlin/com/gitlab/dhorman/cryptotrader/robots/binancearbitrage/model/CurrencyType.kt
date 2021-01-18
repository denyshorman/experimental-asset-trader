package com.gitlab.dhorman.cryptotrader.robots.binancearbitrage.model

enum class CurrencyType {
    Base,
    Quote;

    operator fun not() = when (this) {
        Base -> Quote
        Quote -> Base
    }
}
