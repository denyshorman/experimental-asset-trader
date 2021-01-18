package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.core

import java.math.BigDecimal
import java.math.RoundingMode

val SEVEN_ZEROS_AND_ONE = BigDecimal("0.00000001")
val HALF = BigDecimal("0.5")
val BIG_DECIMAL_TWO = BigDecimal("2")
val INT_MAX_VALUE_BIG_DECIMAL = Int.MAX_VALUE.toBigDecimal()

val BigDecimal.cut8 get() = this.setScale(8, RoundingMode.DOWN)
val BigDecimal.cut8add1 get() = this.setScale(8, RoundingMode.DOWN) + SEVEN_ZEROS_AND_ONE
val BigDecimal.cut8minus1 get() = this.setScale(8, RoundingMode.DOWN) - SEVEN_ZEROS_AND_ONE
val BigDecimal.oneMinus get() = BigDecimal.ONE - this
val BigDecimal.oneMinusAdjPoloniex get() = if (this > HALF) this else this.oneMinus // Workaround for Poloniex because it has error with fee calculation
