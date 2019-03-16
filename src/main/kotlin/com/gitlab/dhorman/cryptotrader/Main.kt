package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.config.HttpServer
import com.gitlab.dhorman.cryptotrader.trader.Trader
import io.vertx.core.Vertx
import mu.KotlinLogging
import org.kodein.di.erased.instance
import reactor.core.publisher.Hooks

fun main() {
    val logger = KotlinLogging.logger {}

    if (logger.isDebugEnabled) Hooks.onOperatorDebug()

    // Schedulers.setFactory(module.schedulersFactory)

    val vertx: Vertx by diContainer.instance()
    val trader: Trader by diContainer.instance()
    val httpServer: HttpServer by diContainer.instance()

    trader.start().subscribe()
    httpServer.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        vertx.close()
    })
}
