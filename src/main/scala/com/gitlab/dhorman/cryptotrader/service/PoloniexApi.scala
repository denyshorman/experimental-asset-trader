package com.gitlab.dhorman.cryptotrader.service

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{Command, TickerData}
import com.typesafe.scalalogging.Logger
import io.circe._
import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax._
import io.vertx.core.http.HttpClientOptions
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.core.http.WebSocket
import reactor.core.publisher.FluxSink
import reactor.core.scala.publisher.{Flux, Mono}

import scala.util.Try


/**
  * Documentation https://poloniex.com/support/api
  */
class PoloniexApi(private val vertx: Vertx) {

  private val logger = Logger[PoloniexApi]

  private val httpClient = vertx.createHttpClient(new HttpClientOptions().setSsl(true))

  private val websocket = Flux.create((sink: FluxSink[WebSocket]) => Mono.from(httpClient
    .websocketStream(443, "api2.poloniex.com", "/")
    .toFlowable)
    .single
    .subscribe(socket => {
      logger.info("WebSocket connection established")

      sink.next(socket)

      socket.closeHandler(_ => {
        logger.info("WebSocket connection closed")
        sink.complete()
      })

      socket.endHandler(_ => {
        logger.info("WebSocket completed transmission")
        sink.complete()
      })

      socket.exceptionHandler(err => {
        logger.error("Exception occurred in WebSocket connection", err)
        sink.error(err)
      })

      sink.onDispose(() => {
        socket.close()
      })
    }, err => {
      sink.error(err)
    })
  )
    .replay(1)
    .refCount()


  private val websocketMessages = websocket
    .flatMap(webSocket => Flux.from(webSocket.toFlowable))
    .map(_.toString)
    .map(parse)
    .flatMap {
      case Left(failure) =>
        logger.error(s"Can't parse received json: $failure")
        Flux.empty[Json]
      case Right(jsonObject) => Flux.just(jsonObject)
    }
    .share()

  val tickerStream: Flux[TickerData] = Flux.create((sink: FluxSink[Seq[TickerData]]) => {
    // Subscribe to ticker stream
    websocket.take(1).subscribe(socket => {
      val jsonStr = Command(Command.Type.Subscribe, Command.Channel.TickerData).asJson.noSpaces
      socket.writeTextMessage(jsonStr)
      logger.info(s"Subscribe to ticker data channel")
    }, err => {
      sink.error(err)
    })

    // Receive ticker data
    val messagesSubscription = websocketMessages
      .filter(isEqual(_, Command.Channel.TickerData))
      .map(TickerData.map)
      .subscribe(tickerData => {
        sink.next(tickerData)
      }, err => {
        sink.error(err)
      }, () => {
        sink.complete()
      })

    // Unsubscribe from ticker data
    sink.onDispose(() => {
      websocket.take(1).subscribe(socket => {
        val jsonStr = Command(Command.Type.Unsubscribe, Command.Channel.TickerData).asJson.noSpaces
        socket.writeTextMessage(jsonStr)
        logger.info("Unsubscribe from ticker data channel")

        messagesSubscription.dispose()
      })
    })
  })
    .flatMapIterable(arr => arr)
    .share()

  private def isEqual(json: Json, commandChannel: Command.Channel): Boolean = json
    .asArray
    .filter(_.nonEmpty)
    .map(_(0))
    .flatMap(_.asNumber)
    .flatMap(_.toInt)
    .contains(commandChannel)
}

object PoloniexApi {
  case class Command(command: Command.Type, channel: Command.Channel)

  case class TickerData(
    currencyPairId: Int,
    lastTradePrice: BigDecimal,
    lowestAsk: BigDecimal,
    highestBid: BigDecimal,
    percentChangeInLast24Hours: BigDecimal,
    baseCurrencyVolumeInLast24Hours: BigDecimal,
    quoteCurrencyVolumeInLast24Hours: BigDecimal,
    isFrozen: Boolean,
    highestTradePriceInLast24Hours: BigDecimal,
    lowestTradePriceInLast24Hours: BigDecimal,
  )

  object Command {

    type Type = String
    type Channel = Int

    object Type {
      val Subscribe = "subscribe"
      val Unsubscribe = "unsubscribe"
    }

    object Channel {
      val AccountNotifications = 1000
      val TickerData = 1002
      val _24HourExchangeVolume = 1003
      val Heartbeat = 1010
    }

  }

  object TickerData {
    private[PoloniexApi] def mapTicker(ticker: Vector[Json]): TickerData = {
      TickerData(
        ticker(0).asNumber.flatMap(x => x.toInt).getOrElse(-1),
        ticker(1).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
        ticker(2).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
        ticker(3).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
        ticker(4).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
        ticker(5).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
        ticker(6).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
        ticker(7).asNumber.flatMap(x => x.toInt).getOrElse(-1) == 1,
        ticker(8).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
        ticker(9).asString.flatMap(n => Try(BigDecimal(n)).toOption).getOrElse(-1),
      )
    }

    private[PoloniexApi] def map(json: Json): Seq[TickerData] = {
      json.asArray.map(_.view.drop(2).flatMap(_.asArray).map(mapTicker)).getOrElse(Seq())
    }
  }
}