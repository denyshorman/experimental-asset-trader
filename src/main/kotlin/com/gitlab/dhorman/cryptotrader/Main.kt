package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.trader.PoloniexTrader
import kotlinx.coroutines.*
import mu.KotlinLogging
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.core.env.Environment
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@SpringBootApplication
class CryptoTraderApplication(
    private val poloniexTrader: PoloniexTrader,
    private val env: Environment
) {
    private val logger = KotlinLogging.logger {}
    private val traderJobs = LinkedList<Job>()

    @PostConstruct
    fun start() {
        if (env.activeProfiles.contains("test")) return

        GlobalScope.launch {
            traderJobs.add(poloniexTrader.start(this))
        }
    }

    @PreDestroy
    fun stop() {
        if (env.activeProfiles.contains("test")) return

        logger.info("Trying to stop all jobs...")

        runBlocking {
            for (job in traderJobs) job.cancelAndJoin()
            traderJobs.clear()
        }

        logger.info("All jobs have been stopped")
    }
}

fun main(args: Array<String>) {
    runApplication<CryptoTraderApplication>(*args)
}
