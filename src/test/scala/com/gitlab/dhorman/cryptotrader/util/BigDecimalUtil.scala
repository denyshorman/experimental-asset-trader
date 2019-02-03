package com.gitlab.dhorman.cryptotrader.util
import scala.math.BigDecimal.RoundingMode

object BigDecimalUtil {
  implicit final class BigDecimalUtil(val num: BigDecimal) extends AnyVal {
    def cut3: BigDecimal = num.setScale(3, RoundingMode.HALF_DOWN)
    def cut8: BigDecimal = num.setScale(8, RoundingMode.HALF_DOWN)
  }
}
