package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model

enum class AmountType {
    From,
    Target;

    operator fun not() = when (this) {
        From -> Target
        Target -> From
    }
}
