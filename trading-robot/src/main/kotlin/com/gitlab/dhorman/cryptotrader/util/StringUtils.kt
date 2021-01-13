package com.gitlab.dhorman.cryptotrader.util

private val hexArray = "0123456789abcdef".toCharArray()

val toHexString: ByteArray.() -> String = {
    val hexChars = CharArray(this.size * 2)
    for (j in this.indices) {
        val v = this[j].toInt() and 0xFF
        hexChars[j * 2] = hexArray[v ushr 4]
        hexChars[j * 2 + 1] = hexArray[v and 0x0F]
    }
    String(hexChars)
}
