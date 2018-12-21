package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.typesafe.scalalogging.Logger
import io.vertx.core
import io.vertx.core.json.JsonObject
import io.vertx.core.{AbstractVerticle, Context}
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import reactor.core.scala.publisher.Flux
import reactor.core.Disposable

import scala.concurrent.duration._

class MainVerticle extends ScalaVerticle {
  private val logger = Logger[MainVerticle]
  private var trader: Trader = _
  private var traderDisposable: Disposable = _

  private def initInternal(): Unit = {
    val main = this

    val module = new MainModule {
      override lazy val vertx: Vertx = main.vertx
    }

    import module._

    trader = new Trader(module.poloniexApi)
    httpServer.hashCode() // init http server hack
    // TODO: init internal components step by step

    // demo values event
    Flux.interval(1.second, module.vertxScheduler).subscribe(v => {
      main.vertx.eventBus().publish("values", new JsonObject().put("value", v))
    })
  }

  override def init(vertx: core.Vertx, context: Context, verticle: AbstractVerticle): Unit = {
    super.init(vertx, context, verticle)
    initInternal()
  }

  override def start(): Unit = {
    logger.info("Start MainVerticle")
    traderDisposable = trader.start().subscribe()
  }

  override def stop(): Unit = {
    logger.info("Stop MainVerticle")
    traderDisposable.dispose()
  }
}
