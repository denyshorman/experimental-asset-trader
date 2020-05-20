package com.gitlab.dhorman.cryptotrader.util

import java.nio.file.Files
import java.nio.file.Path

class CsvGenerator {
    private val sb = StringBuilder()

    fun addLine(vararg cols: Any) {
        var first = true
        for (col in cols) {
            if (first) {
                first = false
            } else {
                sb.append(",")
            }
            when (col) {
                is Number -> {
                    sb.append(col)
                }
                else -> {
                    sb.append("\"")
                    sb.append(col)
                    sb.append("\"")
                }
            }
        }
        sb.appendln()
    }

    fun dumpToString(): String = sb.toString()

    fun dumpToFile(filePath: String) {
        Files.writeString(Path.of(filePath), sb.toString())
    }

    companion object {
        private fun Any.toCsvStr(): String {
            return when (this) {
                is Number -> this.toString()
                else -> "\"$this\""
            }
        }

        fun toCsvLine(vararg cols: Any): String {
            return cols.joinToString(",") { it.toCsvStr() }
        }

        fun toCsvNewLine(vararg cols: Any): String {
            return cols.joinToString(separator = ",", postfix = System.lineSeparator()) { it.toCsvStr() }
        }
    }
}
