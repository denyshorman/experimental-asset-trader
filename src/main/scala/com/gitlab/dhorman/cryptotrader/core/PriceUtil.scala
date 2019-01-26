package com.gitlab.dhorman.cryptotrader.core

import scala.math.BigDecimal.RoundingMode

object PriceUtil {
  private final val _8_1 = BigDecimal(0.00000001)
  private final val _1 = BigDecimal(1)

  implicit final class BigDecimalUtil(val num: BigDecimal) extends AnyVal {
    def cut8: BigDecimal = num.setScale(8, RoundingMode.DOWN)
    def cut8add1: BigDecimal = num.setScale(8, RoundingMode.DOWN) + _8_1
    def oneMinus: BigDecimal = _1 - num
  }
}
