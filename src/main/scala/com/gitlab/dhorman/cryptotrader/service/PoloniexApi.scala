package com.gitlab.dhorman.cryptotrader.service

import com.typesafe.scalalogging.Logger
import io.circe._
import io.circe.parser.parse
import io.reactivex.functions.Consumer
import io.vertx.core.http.HttpClientOptions
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.core.http.WebSocket
import reactor.core.publisher.{Flux, FluxSink}


/**
  * Documentation https://poloniex.com/support/api
  */
class PoloniexApi(private val vertx: Vertx) {

  private val logger = Logger[PoloniexApi]

  private val httpClient = vertx.createHttpClient(new HttpClientOptions().setSsl(true))

  private val websocket: Flux[WebSocket] = Flux.create((sink: FluxSink[WebSocket]) => httpClient
    .websocketStream(443, "api2.poloniex.com", "/")
    .toFlowable
    .singleOrError()
    .subscribe(new Consumer[WebSocket] {
      override def accept(socket: WebSocket): Unit = {
        sink.next(socket)

        socket.closeHandler(_ => {
          sink.complete()
        })

        socket.endHandler(_ => {
          sink.complete()
        })

        socket.exceptionHandler(err => {
          sink.error(err)
        })

        sink.onDispose(() => {
          socket.close()
        })
      }
    }, (err: Throwable) => {
      sink.error(err)
    })
  )
    .replay(1)
    .refCount()


  private val websocketMessages: Flux[Json] = websocket
    .flatMap(_.toFlowable)
    .map[String](_.toString)
    .map[Either[ParsingFailure, Json]](parse(_))
    .flatMap {
      case Left(failure) =>
        logger.error(s"Can't parse received json: $failure")
        Flux.empty[Json]()
      case Right(jsonObject: Json) => Flux.just[Json](jsonObject)
    }

  def tickerStream(): Flux[Json] = {
    // TODO: subscribe
    websocketMessages
    // TODO: Unsubscribe
  }
}

object PoloniexApi {

}