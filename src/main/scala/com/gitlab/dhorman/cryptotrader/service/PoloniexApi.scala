package com.gitlab.dhorman.cryptotrader.service

import com.typesafe.scalalogging.Logger
import io.circe._
import io.circe.parser.parse
import io.vertx.core.http.HttpClientOptions
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.core.http.WebSocket
import reactor.core.publisher.FluxSink
import reactor.core.scala.publisher.Flux


/**
  * Documentation https://poloniex.com/support/api
  */
class PoloniexApi(private val vertx: Vertx) {

  private val logger = Logger[PoloniexApi]

  private val httpClient = vertx.createHttpClient(new HttpClientOptions().setSsl(true))

  private val websocket: Flux[WebSocket] = Flux.create((sink: FluxSink[WebSocket]) => Flux.from(httpClient
    .websocketStream(443, "api2.poloniex.com", "/")
    .toFlowable)
    .subscribe(socket => {
      sink.next(socket)

      socket.closeHandler(_ => {
        logger.debug("Socket closed")
        sink.complete()
      })

      socket.endHandler(_ => {
        logger.debug("Socket completed transmission")
        sink.complete()
      })

      socket.exceptionHandler(err => {
        logger.error("Exception occurred in socket connection", err)
        sink.error(err)
      })

      sink.onDispose(() => {
        socket.close()
      })
    }, err => {
      sink.error(err)
    })
  )
    .retry()
    .replay(1)
    .refCount()


  private val websocketMessages: Flux[Json] = websocket
    .flatMap(websocket => Flux.from(websocket.toFlowable))
    .map(_.toString)
    .map(parse)
    .flatMap {
      case Left(failure) =>
        logger.error(s"Can't parse received json: $failure")
        Flux.empty[Json]
      case Right(jsonObject) => Flux.just(jsonObject)
    }

  def tickerStream(): Flux[Json] = {
    // TODO: subscribe
    websocketMessages
    // TODO: Unsubscribe
  }
}

object PoloniexApi {

}