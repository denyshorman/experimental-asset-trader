package com.gitlab.dhorman.cryptotrader.config

import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.typesafe.scalalogging.Logger
import io.circe.generic.auto._
import io.circe.syntax._
import io.vertx.scala.core.http.ServerWebSocket
import io.vertx.scala.core.{Vertx, http}
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.{ErrorHandler, LoggerHandler, ResponseContentTypeHandler}
import reactor.core.scheduler.Scheduler

import scala.concurrent.duration._

class HttpServer(
  private val vertx: Vertx,
  private val trader: Trader,
  private val vertxScheduler: Scheduler,
) {
  private val server: http.HttpServer = vertx.createHttpServer()
  private val router: Router = Router.router(vertx)
  private val logger: Logger = Logger[HttpServer]

  router.route("/api/*")
    .handler(LoggerHandler.create())
    .handler(ResponseContentTypeHandler.create())
    .failureHandler(ErrorHandler.create())

  router.get("/api/values").handler(rc => {
    rc.response().end("""{"resp":"hello"}""")
  })

  private val priceMovement = trader.indicators.priceMovement
    .buffer(200 millis)
    /*.doOnNext(v => {
      logger.debug(s"stream size: ${v.size}")
    })*/
    .map(_.asJson.noSpaces)
    .share()

  def webSocketHandler(ws: ServerWebSocket): Unit = {
    if (ws.path() == "/api/ws") {
      logger.debug("websocket connection established")

      val sub = priceMovement
        .subscribe(json => {
          ws.writeTextMessage(json)
        }, err => {
          logger.error(err.getMessage, err)
          ws.close()
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
    server.requestHandler(router)
      .websocketHandler(webSocketHandler)
      .listen(8080, "0.0.0.0")
  }
}
