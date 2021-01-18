package com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage

import java.math.BigDecimal

data class FuturesMarketGeneralInfo(
    val makerFee: BigDecimal,
    val takerFee: BigDecimal,
    val minQuoteAmount: BigDecimal,
    val baseAssetPrecision: Int,
    val quotePrecision: Int,
)
