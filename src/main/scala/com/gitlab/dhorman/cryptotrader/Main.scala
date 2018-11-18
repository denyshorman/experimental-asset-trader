package com.gitlab.dhorman.cryptotrader

import com.typesafe.scalalogging.Logger
import io.vertx.scala.core.Vertx

object Main extends App {
  val logger = Logger[Main.type]
  val vertx = Vertx.vertx()
  val mainVerticle = new MainVerticle()
  vertx.deployVerticle(mainVerticle)

  sys.addShutdownHook {
    vertx.close()
  }
}
