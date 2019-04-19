package com.gitlab.dhorman.cryptotrader.core

import java.math.BigDecimal
import java.math.RoundingMode

val SEVEN_ZEROS_AND_ONE = BigDecimal("0.00000001")

val BigDecimal.cut8 get() = this.setScale(8, RoundingMode.DOWN)
val BigDecimal.cut8add1 get() = this.setScale(8, RoundingMode.DOWN) + SEVEN_ZEROS_AND_ONE
val BigDecimal.cut8minus1 get() = this.setScale(8, RoundingMode.DOWN) - SEVEN_ZEROS_AND_ONE
val BigDecimal.oneMinus get() = BigDecimal.ONE - this
