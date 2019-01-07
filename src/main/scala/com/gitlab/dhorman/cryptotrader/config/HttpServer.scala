package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.typesafe.scalalogging.Logger
import io.vertx.scala.core.http.ServerWebSocket
import io.vertx.scala.core.{Vertx, http}
import io.vertx.scala.ext.bridge.PermittedOptions
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.{ErrorHandler, LoggerHandler, ResponseContentTypeHandler}
import io.vertx.scala.ext.web.handler.sockjs.{BridgeOptions, SockJSHandler}
import reactor.core.scheduler.Scheduler
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.duration._

class HttpServer(
  private val vertx: Vertx,
  private val trader: Trader,
  private val vertxScheduler: Scheduler,
) {
  private val server: http.HttpServer = vertx.createHttpServer()
  private val router: Router = Router.router(vertx)
  private val logger: Logger = Logger[HttpServer]

  private val options = BridgeOptions()
    .addOutboundPermitted(PermittedOptions()
      .setAddress("values"))

  private val sockJSHandler = SockJSHandler.create(vertx).bridge(options)

  router.route("/api/*")
    .handler(LoggerHandler.create())
    .handler(ResponseContentTypeHandler.create())
    .failureHandler(ErrorHandler.create())

  router.route("/eventbus/*").handler(sockJSHandler)

  router.get("/api/values").handler(rc => {
    rc.response().end("""{"resp":"hello"}""")
  })


  def webSocketHandler(ws: ServerWebSocket): Unit = {
    if (ws.path() == "/api/ws") {
      logger.debug("websocket connection established")

      val sub = trader.indicators.priceMovement
        .onBackpressureLatest()
        .buffer(3 seconds)
        .subscribe(valuation => {
          val json = valuation.asJson.noSpaces
          ws.writeTextMessage(json)
        })

      ws.textMessageHandler(msg => {
        logger.debug(s"message received: $msg")
      })

      ws.closeHandler(_ => {
        logger.debug("websocket connection closed")
        sub.dispose()
      })
    } else {
      ws.reject()
    }
  }

  def start(): Unit = {
    server.requestHandler(router).websocketHandler(webSocketHandler).listen(8080)
  }
}
