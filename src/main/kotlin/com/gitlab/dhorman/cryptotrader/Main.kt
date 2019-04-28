package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.config.HttpServer
import com.gitlab.dhorman.cryptotrader.trader.PoloniexTrader
import com.gitlab.dhorman.cryptotrader.util.FlowScope
import io.vertx.core.Vertx
import kotlinx.coroutines.cancel
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
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
    val poloniexTrader: PoloniexTrader by diContainer.instance()
    val httpServer: HttpServer by diContainer.instance()

    runBlocking(Schedulers.parallel().asCoroutineDispatcher()) {
        httpServer.start()
        poloniexTrader.start().awaitSingle()
    }

    Runtime.getRuntime().addShutdownHook(Thread {
        vertx.close()
        FlowScope.cancel()
    })
}
