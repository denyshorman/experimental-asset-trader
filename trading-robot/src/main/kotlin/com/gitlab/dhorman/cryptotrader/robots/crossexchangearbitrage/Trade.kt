package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import java.math.BigDecimal

data class Trade(
    val quoteAmount: BigDecimal,
    val price: BigDecimal,
    val fee: BigDecimal,
)
