package com.gitlab.dhorman.cryptotrader

import com.typesafe.scalalogging.Logger
import io.vertx.reactivex.core.Vertx


object Main extends App {
  val vertx = Vertx.vertx()
  val logger = Logger[Main.type]
}
