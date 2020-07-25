package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage.model

enum class OrderType {
    Sell,
    Buy;

    operator fun not() = when (this) {
        Sell -> Buy
        Buy -> Sell
    }
}
