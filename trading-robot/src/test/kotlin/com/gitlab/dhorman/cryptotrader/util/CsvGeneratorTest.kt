package com.gitlab.dhorman.cryptotrader.util

import org.junit.jupiter.api.Test
import java.math.BigDecimal

class CsvGeneratorTest {
    @Test
    fun testDumpToFile() {
        val csvGenerator = CsvGenerator()
        csvGenerator.addLine("one", "two", "three", "four")
        csvGenerator.addLine("data1", "data2", BigDecimal("13.3333333"), 3L)
        csvGenerator.addLine("data3", "data4", BigDecimal("9.3333333"), 10L)
        csvGenerator.dumpToFile("build/reports/test.csv")
    }
}
