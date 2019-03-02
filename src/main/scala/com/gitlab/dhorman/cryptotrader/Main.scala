package com.gitlab.dhorman.cryptotrader

import com.typesafe.scalalogging.Logger
import io.vertx.scala.core.Vertx
import reactor.core.publisher.Hooks

object Main extends App {
  val logger = Logger[Main.type]
  val vertx = Vertx.vertx()

  val module = new MainModule {
    override lazy val vertx: Vertx = Main.vertx
  }

  logger.whenDebugEnabled {
    Hooks.onOperatorDebug()
  }

  // Schedulers.setFactory(module.schedulersFactory)

  module.trader.start().subscribe()
  module.httpServer.start()

  sys.addShutdownHook {
    vertx.close()
  }
}
