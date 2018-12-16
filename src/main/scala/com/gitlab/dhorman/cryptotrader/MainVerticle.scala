package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.typesafe.scalalogging.Logger
import io.vertx.core
import io.vertx.core.json.JsonObject
import io.vertx.core.{AbstractVerticle, Context}
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.sockjs.{BridgeOptions, SockJSHandler}
import io.vertx.scala.ext.web.handler.{ErrorHandler, LoggerHandler, ResponseContentTypeHandler}
import reactor.core.scala.publisher.Flux
import io.vertx.scala.ext.bridge.PermittedOptions

import scala.concurrent.duration._

class MainVerticle extends ScalaVerticle {
  private val logger = Logger[MainVerticle]
  private var trader: Trader = _
  private var module: MainModule = _

  private def initInternal(): Unit = {
    val main = this

    val module = new MainModule {
      override lazy val vertx: Vertx = main.vertx
    }

    import module._

    trader = new Trader(module.poloniexApi)

    this.module = module
  }


  override def init(vertx: core.Vertx, context: Context, verticle: AbstractVerticle): Unit = {
    super.init(vertx, context, verticle)
    initInternal()
  }

  override def start(): Unit = {
    logger.info("Start MainVerticle")
    trader.start()

    Flux.interval(1.second, module.vertxScheduler).subscribe(v => {
      vertx.eventBus().publish("values", new JsonObject().put("value", v))
    })

    val server = vertx.createHttpServer()
    val router = Router.router(vertx)

    router.route("/api/*")
      .handler(LoggerHandler.create())
      .handler(ResponseContentTypeHandler.create())
      .failureHandler(ErrorHandler.create())

    val options = BridgeOptions()
      .addOutboundPermitted(PermittedOptions()
        .setAddress("values"))

    val sockJSHandler = SockJSHandler.create(vertx).bridge(options)

    router.route("/eventbus/*").handler(sockJSHandler)

    router.get("/api/values").handler(rc => {
      rc.response().end("""{"resp":"hello"}""")
    })

    server.requestHandler(router).listen(8080)
  }

  override def stop(): Unit = {
    logger.info("Stop MainVerticle")
    trader.stop()
  }
}
