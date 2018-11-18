package com.gitlab.dhorman.cryptotrader

import io.vertx.scala.core.Vertx

trait TestModule extends MainModule {
  lazy val vertx: Vertx = Vertx.vertx()
}
