package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.trader.PoloniexTrader
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@SpringBootApplication
class CryptoTraderApplication(
    private val poloniexTrader: PoloniexTrader
) {
    private val traderJobs = LinkedList<Job>()

    @PostConstruct
    fun start() {
        GlobalScope.launch {
            traderJobs.add(poloniexTrader.start(this))
        }
    }

    @PreDestroy
    fun stop() {
        runBlocking {
            for (job in traderJobs) job.join()
            traderJobs.clear()
        }
    }
}

fun main(args: Array<String>) {
    runApplication<CryptoTraderApplication>(*args)
}
