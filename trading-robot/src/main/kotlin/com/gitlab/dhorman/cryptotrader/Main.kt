package com.gitlab.dhorman.cryptotrader

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CryptoTraderApplication

fun main(args: Array<String>) {
    runApplication<CryptoTraderApplication>(*args)
}
