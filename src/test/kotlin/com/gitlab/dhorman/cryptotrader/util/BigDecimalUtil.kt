package com.gitlab.dhorman.cryptotrader.util

import java.math.BigDecimal
import java.math.RoundingMode

val BigDecimal.cut3 get() = this.setScale(3, RoundingMode.HALF_DOWN)
val BigDecimal.cut8 get() = this.setScale(8, RoundingMode.HALF_DOWN)