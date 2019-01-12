package com.gitlab.dhorman.cryptotrader.config

import java.util.concurrent.ConcurrentHashMap

import com.gitlab.dhorman.cryptotrader.config.HttpServer.{Msg, ReqMsg, RespMsg}
import com.gitlab.dhorman.cryptotrader.trader.Trader
import com.typesafe.scalalogging.Logger
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.extras._
import io.circe.generic.semiauto._
import io.circe.optics.JsonPath._
import io.circe.parser.parse
import io.circe.syntax._
import io.vertx.scala.core.http.ServerWebSocket
import io.vertx.scala.core.{Vertx, http}
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.{ErrorHandler, LoggerHandler, ResponseContentTypeHandler}
import reactor.core.Disposable
import reactor.core.scheduler.Scheduler

import scala.concurrent.duration._
import scala.util.Try

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

  private val streams = Map(
    Msg.Ticker -> trader.data.tickers
      .sample(1 second)
      .map(data => RespMsg(Msg.Ticker, data))
      .map(_.asJson.noSpaces)
      .share(),
    Msg.PriceMovement -> trader.indicators.priceMovement
      .buffer(200 millis)
      .map(data => RespMsg(Msg.PriceMovement, data))
      .map(_.asJson.noSpaces)
      .share()
  )

  def webSocketHandler(ws: ServerWebSocket): Unit = {
    if (ws.path() == "/api/ws") {
      logger.debug("websocket connection established")

      val map = new ConcurrentHashMap[Int, Disposable]()

      ws.textMessageHandler(msg => {
        parse(msg) match {
          case Left(err) => // parse error
          case Right(value) =>
            value.as[ReqMsg].toOption match {
              case Some(req) =>
                req.`type` match {
                  case Msg.Subscribe =>
                    logger.whenDebugEnabled{
                      logger.debug(s"subscribe to ${req.id}")
                    }

                    req.id match {
                      case Msg.Ticker =>
                        val disposable = streams(Msg.Ticker).subscribe(json => {
                          Try{ws.writeTextMessage(json)}
                        }, e => {
                          logger.error(e.getMessage, e)
                        })

                        map.put(Msg.Ticker, disposable)
                      case Msg.PriceMovement =>
                        val disposable = streams(Msg.PriceMovement).subscribe(json => {
                          Try{ws.writeTextMessage(json)}
                        }, e => {
                          logger.error(e.getMessage, e)
                        })

                        map.put(Msg.PriceMovement, disposable)
                    }
                  case Msg.Unsubscribe =>
                    val dis = map.get(req.id)
                    if (dis != null) {
                      map.remove(req.id)
                      dis.dispose()
                    }

                    logger.whenDebugEnabled{
                      logger.debug(s"unsubscribe from ${req.id}")
                    }
                }
              case None => // parse error
            }
        }
      })

      ws.closeHandler(_ => {
        logger.debug("websocket connection closed")
        map.forEach((_: Int, d: Disposable) => d.dispose())
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

object HttpServer {
  case class ReqMsg(`type`: Int, id: Int)
  case class RespMsg[T](id: Int, data: T)

  object Msg {
    val Subscribe = 0
    val Unsubscribe = 1

    val Ticker = 0
    val PriceMovement = 1
  }
}