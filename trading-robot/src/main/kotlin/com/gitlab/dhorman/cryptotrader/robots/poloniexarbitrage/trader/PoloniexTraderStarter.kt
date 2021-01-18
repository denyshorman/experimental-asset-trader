package com.gitlab.dhorman.cryptotrader.robots.poloniexarbitrage.trader

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.annotation.Profile
import org.springframework.context.event.ContextClosedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
@Profile("!test")
class PoloniexTraderStarter(private val poloniexTrader: PoloniexTrader) {
    private val logger = KotlinLogging.logger {}

    @Volatile
    private var job: Job? = null

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        job = poloniexTrader.start(GlobalScope)
    }

    @EventListener(ContextClosedEvent::class)
    fun close() = runBlocking {
        logger.info("Trying to stop Poloniex trader job...")

        job?.cancelAndJoin()
        job = null

        logger.info("Poloniex trader job has been stopped")
    }
}
