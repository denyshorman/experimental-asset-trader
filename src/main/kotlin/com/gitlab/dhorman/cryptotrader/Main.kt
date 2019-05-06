package com.gitlab.dhorman.cryptotrader

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@SpringBootApplication
class CryptoTraderApplication {
    @PostConstruct
    fun start() {
    }

    @PreDestroy
    fun stop() {
    }
}

fun main(args: Array<String>) {
    runApplication<CryptoTraderApplication>(*args)
}
