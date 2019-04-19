package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.config.HttpServer
import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import io.vertx.core.Vertx
import kotlinx.coroutines.cancel
import mu.KotlinLogging
import org.kodein.di.erased.instance
import reactor.core.publisher.Hooks
import reactor.core.scheduler.Schedulers

fun main() {
    val logger = KotlinLogging.logger {}

    if (logger.isDebugEnabled) Hooks.onOperatorDebug()

    /*val reactorSchedulersFactory: Schedulers.Factory by diContainer.instance()
    Schedulers.setFactory(reactorSchedulersFactory)*/

    val vertx: Vertx by diContainer.instance()
    val trader: Trader by diContainer.instance()
    val httpServer: HttpServer by diContainer.instance()

    trader.start().subscribe()
    httpServer.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        vertx.close()
        FlowScope.cancel()
    })
}
