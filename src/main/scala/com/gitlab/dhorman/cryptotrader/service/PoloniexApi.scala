package com.gitlab.dhorman.cryptotrader.service

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime}

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi._
import com.roundeights.hasher.Implicits._
import com.typesafe.scalalogging.Logger
import io.circe._
import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax._
import io.circe.optics.JsonPath._
import io.vertx.core.http.HttpClientOptions
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.core.http.WebSocket
import io.vertx.scala.core.{MultiMap => MultiMapScala, Vertx => VertxScala}
import io.vertx.scala.ext.web.client.{WebClient, WebClientOptions}
import reactor.core.publisher.FluxSink
import reactor.core.scala.publisher.{Flux, Mono}

import scala.concurrent.ExecutionContext
import scala.util.Try


/**
  * Documentation https://poloniex.com/support/api
  */
class PoloniexApi(private val vertx: Vertx) {

  private val apiKey = "ANGSUCQR-PROSS906-RL9U5FW5-QB4WTXHU"
  private val apiSecret = "49927c9945ff03575a69b8150f08990535fa040e84d206ea51da2d52299de788712977c183a53f9f20500995a7ebcb8dc4306539b2d94a90263ca0577f91aa37"

  private val scalaVertx = VertxScala(vertx.getDelegate)

  private implicit val ec: ExecutionContext = VertxExecutionContext(scalaVertx.getOrCreateContext())

  private val logger = Logger[PoloniexApi]

  private val httpClient = vertx.createHttpClient(new HttpClientOptions().setSsl(true))

  private val webclient = {
    val options = WebClientOptions()
      .setKeepAlive(false)
      .setSsl(true)
    WebClient.create(scalaVertx)
  }

  private val websocket = Flux.create((sink: FluxSink[WebSocket]) => Mono.from(httpClient
    .websocketStream(443, "api2.poloniex.com", "/")
    .toFlowable)
    .single
    .subscribe((socket: WebSocket) => {
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

  val tickerStream: Flux[TickerData] = Flux.create(create(Command.Channel.TickerData, TickerData.map))
    .flatMapIterable(arr => arr)
    .share()

  val _24HourExchangeVolumeStream: Flux[_24HourExchangeVolume] = Flux.create(create(Command.Channel._24HourExchangeVolume, _24HourExchangeVolume.map))
    .filter(_.isDefined)
    .map(_.get)
    .share()

  def orderBookStream(currencyPair: String): Flux[OrderBook] = {
    ???
  }

  /**
    * Returns all of your available balances
    */
  def balances(): Mono[Map[String, BigDecimal]] = {
    callPrivateApi("returnBalances").map(mapJsonToObject[Map[String, BigDecimal]])
  }

  /**
    * Returns all of your balances, including available balance, balance on orders, and the estimated BTC value of your balance.
    */
  def completeBalances(): Mono[Map[String, CompleteBalance]] = {
    callPrivateApi("returnCompleteBalances").map(mapJsonToObject[Map[String, CompleteBalance]])
  }

  /**
    * Returns all of your deposit addresses.
    */
  def depositAddresses(): Mono[Map[String, String]] = {
    callPrivateApi("returnDepositAddresses").map(mapJsonToObject[Map[String, String]])
  }

  /**
    * Returns your open orders for a given market, specified by the currencyPair.
    */
  def openOrders(currencyPair: String): Mono[List[OpenOrder]] = {
    callPrivateApi("returnOpenOrders", Map("currencyPair" -> currencyPair))
      .map(mapJsonToObject[List[OpenOrder]])
  }

  def allOpenOrders(): Mono[Map[String, List[OpenOrder]]] = {
    callPrivateApi("returnOpenOrders", Map("currencyPair" -> "all"))
      .map(mapJsonToObject[Map[String, List[OpenOrder]]])
  }

  private def mapJsonToObject[T](json: Json)(implicit decoder: Decoder[T]) : T = json.as[T] match {
    case Left(err) => throw err
    case Right(value) => value
  }

  private def callPrivateApi(methodName: String, postArgs: Map[String, String] = Map()): Mono[Json] = {
    val postParamsPrivate = Map("command" -> methodName, "nonce" -> Instant.now.getEpochSecond.toString)
    val postParams = postParamsPrivate ++ postArgs
    val sign = postParams.view.map(p => s"${p._1}=${p._2}").mkString("&")

    val req = webclient
      .post(443, "poloniex.com", "/tradingApi")
      .ssl(true)
      .putHeader("Key", apiKey)
      .putHeader("Sign", sign.hmac(apiSecret).sha512)

    val reqBody = MultiMapScala.caseInsensitiveMultiMap()
    postParams.foreach(p => reqBody.set(p._1, p._2))

    Mono.fromFuture(req.sendFormFuture(reqBody))
      .map(resp => {
        val bodyOpt = resp.bodyAsString()
        if (bodyOpt.isEmpty) throw new NoSuchElementException(s"Body response is empty for command $methodName")

        parse(bodyOpt.get) match {
          case Left(failure) => throw failure
          case Right(json) => json
        }
      })
      .map(json => {
        val errorMsgOpt = json
          .asObject
          .flatMap(_("error"))
          .flatMap(_.asString)

        errorMsgOpt match {
          case Some(errorMsg) => throw new Exception(errorMsg)
          case None => json
        }
      })
  }

  private def create[T](channel: Command.Channel, mapper: Json => T): FluxSink[T] => Unit = (sink: FluxSink[T]) => {
    // Subscribe to ticker stream
    websocket.take(1).subscribe(socket => {
      val jsonStr = Command(Command.Type.Subscribe, channel).asJson.noSpaces
      socket.writeTextMessage(jsonStr)
      logger.info(s"Subscribe to $channel channel")
    }, err => {
      sink.error(err)
    })

    // Receive ticker data
    val messagesSubscription = websocketMessages
      .filter(isEqual(_, channel))
      .map(mapper)
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
        val jsonStr = Command(Command.Type.Unsubscribe, channel).asJson.noSpaces
        socket.writeTextMessage(jsonStr)
        logger.info(s"Unsubscribe from $channel channel")

        messagesSubscription.dispose()
      })
    })
  }

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

  object TickerData {
    private[PoloniexApi] def mapTicker(ticker: Vector[Json]): TickerData = TickerData(
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

    private[PoloniexApi] def map(json: Json): Seq[TickerData] = {
      json.asArray.map(_.view.drop(2).flatMap(_.asArray).map(mapTicker)).getOrElse(Seq())
    }
  }

  case class _24HourExchangeVolume(
    time: LocalDateTime,
    usersOnline: Int,
    baseCurrency24HVolume: Map[String, BigDecimal],
  )

  object _24HourExchangeVolume {
    private[PoloniexApi] def mapDate(json: Json): LocalDateTime = {
      json.asString
        .flatMap(dateStr =>
          Try(
            LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
          ).toOption
        )
        .getOrElse(LocalDateTime.now())
    }

    private[PoloniexApi] def mapInt(json: Json): Int = {
      json.asNumber.flatMap(num => num.toInt).getOrElse(-1)
    }

    private[PoloniexApi] def mapBaseCurrencies24HVolume(json: Json) : Map[String, BigDecimal] = {
      json
        .asObject
        .map(_.toMap.map(v => (v._1, v._2
          .asString
          .flatMap(amount => Try(BigDecimal(amount)).toOption)
          .getOrElse(BigDecimal(-1))))
        )
        .getOrElse(Map())
    }

    private[PoloniexApi] def map(json: Json): Option[_24HourExchangeVolume] = {
      json.asArray.flatMap(_.view
        .drop(2)
        .flatMap(_.asArray)
        .filter(_.size >= 3)
        .map(arr => _24HourExchangeVolume(mapDate(arr(0)), mapInt(arr(1)), mapBaseCurrencies24HVolume(arr(2))))
        .headOption
      )
    }

  }


  case class OrderBook(

  )

  object OrderBook {

  }

  case class CompleteBalance(available: BigDecimal, onOrders: BigDecimal, btcValue: BigDecimal)

  case class OpenOrder(
    orderNumber: String,
    `type`: String, // sell/buy
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal,
  )

}