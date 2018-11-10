package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.typesafe.scalalogging.Logger


object Main extends App with MainModule {
  val logger = Logger[Main.type]

  val trader = new Trader(poloniexApi)

  scalaVertx.deployVerticle(trader)

  sys.addShutdownHook {
    vertx.close()
  }
}
