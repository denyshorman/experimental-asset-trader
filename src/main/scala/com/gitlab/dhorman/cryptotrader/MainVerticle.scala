package com.gitlab.dhorman.cryptotrader

import com.typesafe.scalalogging.Logger
import io.vertx.core
import io.vertx.core.{AbstractVerticle, Context}
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx

class MainVerticle extends ScalaVerticle {
  private val logger = Logger[MainVerticle]
  private var module: MainModule = _

  private def initInternal(): Unit = {
    val main = this

    val module = new MainModule {
      override lazy val vertx: Vertx = main.vertx
    }

    this.module = module
  }

  override def init(vertx: core.Vertx, context: Context, verticle: AbstractVerticle): Unit = {
    super.init(vertx, context, verticle)
    initInternal()
  }

  override def start(): Unit = {
    logger.info("Start MainVerticle")
    module.trader.start().subscribe()
    module.httpServer.start()
  }

  override def stop(): Unit = {
    logger.info("Stop MainVerticle")
  }
}
