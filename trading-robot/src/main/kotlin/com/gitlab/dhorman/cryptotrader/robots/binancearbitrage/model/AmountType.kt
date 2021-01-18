package com.gitlab.dhorman.cryptotrader.robots.binancearbitrage.model

enum class AmountType {
    From,
    Target;

    operator fun not() = when (this) {
        From -> Target
        Target -> From
    }
}
