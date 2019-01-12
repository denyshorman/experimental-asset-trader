package com.gitlab.dhorman.cryptotrader

import com.typesafe.scalalogging.Logger
import io.vertx.scala.core.Vertx
import reactor.core.scheduler.Schedulers

object Main extends App {
  val logger = Logger[Main.type]
  val vertx = Vertx.vertx()

  val module = new MainModule {
    override lazy val vertx: Vertx = Main.vertx
  }

  //Hooks.onOperatorDebug()
  Schedulers.setFactory(module.schedulersFactory)

  module.trader.start().subscribe()
  module.httpServer.start()

  sys.addShutdownHook {
    vertx.close()
  }
}
