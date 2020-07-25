package com.gitlab.dhorman.cryptotrader.robots.binance.arbitrage

import com.gitlab.dhorman.cryptotrader.service.binance.BinanceApi
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
class Starter(private val binanceApi: BinanceApi) {
    private val logger = KotlinLogging.logger {}

    @Volatile
    private var job: Job? = null

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        val robot = Robot(binanceApi)
        job = robot.start(GlobalScope)
    }

    @EventListener(ContextClosedEvent::class)
    fun close() = runBlocking {
        logger.info("Trying to stop Binance arbitrage robot...")

        job?.cancelAndJoin()
        job = null

        logger.info("Binance arbitrage robot has been stopped")
    }
}
