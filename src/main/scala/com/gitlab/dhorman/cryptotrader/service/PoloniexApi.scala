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
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientOptions
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.reactivex.core.{Vertx => VertxRx}
import io.vertx.reactivex.core.http.WebSocket
import io.vertx.scala.core.{MultiMap => MultiMapScala, Vertx => VertxScala}
import io.vertx.scala.ext.web.client.{HttpResponse, WebClient, WebClientOptions}
import reactor.core.publisher.FluxSink
import reactor.core.scala.publisher.{Flux, Mono}
import com.softwaremill.tagging._
import cats.syntax.either._

import scala.concurrent.ExecutionContext
import scala.util.Try


/**
  * Documentation https://poloniex.com/support/api
  */
class PoloniexApi(
  private val vertx: Vertx,
  private val poloniexApiKey: String @@ PoloniexApiKeyTag,
  private val poloniexApiSecret: String @@ PoloniexApiSecretTag,
) {
  private val logger = Logger[PoloniexApi]

  private val vertxRx = new VertxRx(vertx)
  private val scalaVertx = VertxScala(vertx)
  private implicit val ec: ExecutionContext = VertxExecutionContext(scalaVertx.getOrCreateContext())

  private val PoloniexPrivatePublicHttpApiUrl = "poloniex.com"
  private val PoloniexWebSocketApiUrl = "api2.poloniex.com"

  private val httpClient = vertxRx.createHttpClient(new HttpClientOptions().setSsl(true))

  private val webclient = {
    val options = WebClientOptions()
      .setKeepAlive(false)
      .setSsl(true)

    WebClient.create(scalaVertx)
  }

  private val websocket = Flux.create((sink: FluxSink[WebSocket]) => Mono.from(httpClient
    .websocketStream(443, PoloniexWebSocketApiUrl, "/")
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
    *
    * @return Returns the ticker for all markets.
    */
  def ticker(): Mono[Map[Market, Ticker]] = {
    val command = "returnTicker"
    val jsonToObjectMapper = mapJsonToObject[Map[Market, Ticker]]
    callPublicApi(command).map(jsonToObjectMapper)
  }

  // TODO: Incorrect json response
  /**
    *
    * @return Returns the 24-hour volume for all markets, plus totals for primary currencies.
    */
  def get24Volume(): Mono[Map[Market, Map[Currency, BigDecimal]]] = {
    val command = "return24Volume"
    val jsonToObjectMapper = mapJsonToObject[Map[Market, Map[Currency, BigDecimal]]]
    callPublicApi(command).map(jsonToObjectMapper)
  }

  def orderBook(currencyPair: Option[String], depth: Int): Mono[Map[Market, OrderBook]] = {
    val command = "returnOrderBook"
    val params = Map("currencyPair" -> currencyPair.getOrElse("all"), "depth" -> depth.toString)
    if (currencyPair.isEmpty) {
      val jsonToObjectMapper = mapJsonToObject[Map[Market, OrderBook]]
      callPublicApi(command, params).map(jsonToObjectMapper)
    } else {
      val jsonToObjectMapper = mapJsonToObject[OrderBook]
      callPublicApi(command, params).map(jsonToObjectMapper).map(orderBook => Map(currencyPair.get -> orderBook))
    }
  }

  def tradeHistoryPublic(market: Market, fromDate: Option[Long], toDate: Option[Long]): Mono[Array[TradeHistory]] = {
    val command = "returnTradeHistory"

    val params = Map(
      "currencyPair" -> Some(market),
      "start" -> fromDate,
      "end" -> toDate,
    )
      .view
      .filter(_._2.isDefined)
      .map(v => (v._1, v._2.get.toString))
      .toMap

    val jsonToObjectMapper = mapJsonToObject[Array[TradeHistory]]
    callPublicApi(command, params).map(jsonToObjectMapper)
  }

  /**
    * @return Returns candlestick chart data. Required GET parameters are "currencyPair", "period" (candlestick period in seconds; valid values are 300, 900, 1800, 7200, 14400, and 86400), "start", and "end". "Start" and "end" are given in UNIX timestamp format and used to specify the date range for the data returned.
    */
  def chartData(market: Market, period: Int, fromDate: Option[Long], toDate: Option[Long]): Mono[Array[ChartData]] = {
    val command = "returnChartData"

    val params = Map(
      "currencyPair" -> Some(market),
      "period" -> Some(period),
      "start" -> fromDate,
      "end" -> toDate,
    )
      .view
      .filter(_._2.isDefined)
      .map(v => (v._1, v._2.get.toString))
      .toMap

    val jsonToObjectMapper = mapJsonToObject[Array[ChartData]]
    callPublicApi(command, params).map(jsonToObjectMapper)
  }

  def currencies(): Mono[Map[Currency, CurrencyDetails]] = {
    val command = "returnCurrencies"
    val jsonToObjectMapper = mapJsonToObject[Map[Currency, CurrencyDetails]]
    callPrivateApi(command).map(jsonToObjectMapper)
  }

  def loanOrders(currency: Currency): Mono[LoanOrder] = {
    val command = "returnLoanOrders"
    val params = Map("currency" -> currency)
    val jsonToObjectMapper = mapJsonToObject[LoanOrder]
    callPublicApi(command, params).map(jsonToObjectMapper)
  }

  /**
    * Returns all of your available balances
    */
  def balances(): Mono[Map[Currency, BigDecimal]] = {
    callPrivateApi("returnBalances").map(mapJsonToObject[Map[Currency, BigDecimal]])
  }

  /**
    * Returns all of your balances, including available balance, balance on orders, and the estimated BTC value of your balance.
    */
  def completeBalances(): Mono[Map[Currency, CompleteBalance]] = {
    callPrivateApi("returnCompleteBalances").map(mapJsonToObject[Map[Currency, CompleteBalance]])
  }

  /**
    * Returns all of your deposit addresses.
    */
  def depositAddresses(): Mono[Map[Currency, String]] = {
    callPrivateApi("returnDepositAddresses").map(mapJsonToObject[Map[Currency, String]])
  }


  def generateNewAddress(currency: Currency): Mono[String] = {
    val command = "generateNewAddress"
    val params = Map("currency" -> currency)
    val jsonToObjectMapper = mapJsonToObject[NewAddressGenerated]
    callPrivateApi(command, params).map(jsonToObjectMapper).map(_.response) // TODO: check success flag
  }

  def depositsWithdrawals(fromDate: Long, toDate: Long): Mono[DepositsWithdrawals] = {
    val command = "returnDepositsWithdrawals"
    val params = Map("start" -> fromDate.toString, "end" -> toDate.toString)
    val jsonToObjectMapper = mapJsonToObject[DepositsWithdrawals]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  /**
    * Returns your open orders for a given market, specified by the currencyPair.
    */
  def openOrders(currencyPair: String): Mono[List[OpenOrder]] = {
    callPrivateApi("returnOpenOrders", Map("currencyPair" -> currencyPair))
      .map(mapJsonToObject[List[OpenOrder]])
  }

  def allOpenOrders(): Mono[Map[Market, List[OpenOrder]]] = {
    val command = "returnOpenOrders"
    val params = Map("currencyPair" -> "all")
    val jsonToObjectMapper = mapJsonToObject[Map[Market, List[OpenOrder]]]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  def tradeHistory(market: Option[Market]): Mono[Map[Market, List[TradeHistoryPrivate]]] = {
    val command = "returnTradeHistory"
    val params = Map("currencyPair" -> market.getOrElse("all"))
    if (market.isEmpty) {
      val jsonToObjectMapper = mapJsonToObject[Map[Market, List[TradeHistoryPrivate]]]
      callPrivateApi(command, params).map(jsonToObjectMapper)
    } else {
      val jsonToObjectMapper = mapJsonToObject[List[TradeHistoryPrivate]]
      callPrivateApi(command, params).map(jsonToObjectMapper).map(v => Map(market.get -> v))
    }
  }

  def orderTrades(orderNumber: BigDecimal): Mono[List[OrderTrade]] = {
    val command = "returnOrderTrades"
    val params = Map("orderNumber" -> orderNumber.toString)
    val jsonToObjectMapper = mapJsonToObject[List[OrderTrade]]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def orderStatus(): Mono[Unit] = {
    val command = "returnOrderStatus"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def buy(market: Market, rate: BigDecimal, amount: BigDecimal): Mono[Buy] = {
    val command = "buy"
    val params = Map("currencyPair" -> market, "rate" -> rate.toString, "amount" -> amount.toString)
    val jsonToObjectMapper = mapJsonToObject[Buy]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def sell(): Mono[Unit] = {
    val command = "sell"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def cancelOrder(): Mono[Unit] = {
    val command = "cancelOrder"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def moveOrder(): Mono[Unit] = {
    val command = "moveOrder"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def withdraw(): Mono[Unit] = {
    val command = "withdraw"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def feeInfo(): Mono[Unit] = {
    val command = "returnFeeInfo"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  def availableAccountBalances(): Mono[AvailableAccountBalance] = {
    val command = "returnAvailableAccountBalances"
    val jsonToObjectMapper = mapJsonToObject[AvailableAccountBalance]
    callPrivateApi(command).map(jsonToObjectMapper)
  }

  def tradableBalances(): Mono[Map[Market, Map[Currency, BigDecimal]]] = {
    val command = "returnTradableBalances"
    val jsonToObjectMapper = mapJsonToObject[Map[Market, Map[Currency, BigDecimal]]]
    callPrivateApi(command).map(jsonToObjectMapper)
  }

  // TODO:
  def transferBalance(): Mono[Unit] = {
    val command = "transferBalance"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def marginAccountSummary(): Mono[Unit] = {
    val command = "returnMarginAccountSummary"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def marginBuy(): Mono[Unit] = {
    val command = "marginBuy"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def marginSell(): Mono[Unit] = {
    val command = "marginSell"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def marginPosition(): Mono[Unit] = {
    val command = "getMarginPosition"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def closeMarginPosition(): Mono[Unit] = {
    val command = "closeMarginPosition"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def createLoanOffer(): Mono[Unit] = {
    val command = "createLoanOffer"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def cancelLoanOffer(): Mono[Unit] = {
    val command = "cancelLoanOffer"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def openLoanOffers(): Mono[Unit] = {
    val command = "returnOpenLoanOffers"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def activeLoans(): Mono[Unit] = {
    val command = "returnActiveLoans"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def lendingHistory(): Mono[Unit] = {
    val command = "returnLendingHistory"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def toggleAutoRenew(): Mono[Unit] = {
    val command = "toggleAutoRenew"
    val params = Map[String,String]()
    val jsonToObjectMapper = mapJsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  private def callPublicApi(command: String, queryParams: Map[String, String] = Map()): Mono[Json] = {
    val qParams = (Map("command" -> command) ++ queryParams).view.map(p => s"${p._1}=${p._2}").mkString("&")

    Mono.fromFuture(
      webclient
        .get(443, PoloniexPrivatePublicHttpApiUrl, s"/public?$qParams")
        .ssl(true)
        .sendFuture()
    )
      .map(bodyToJson)
      .map(handleErrorResp)
  }

  private def callPrivateApi(methodName: String, postArgs: Map[String, String] = Map()): Mono[Json] = {
    val postParamsPrivate = Map("command" -> methodName, "nonce" -> Instant.now.getEpochSecond.toString)
    val postParams = postParamsPrivate ++ postArgs
    val sign = postParams.view.map(p => s"${p._1}=${p._2}").mkString("&")

    val req = webclient
      .post(443, PoloniexPrivatePublicHttpApiUrl, "/tradingApi")
      .ssl(true)
      .putHeader("Key", poloniexApiKey)
      .putHeader("Sign", sign.hmac(poloniexApiSecret).sha512)

    val reqBody = MultiMapScala.caseInsensitiveMultiMap()
    postParams.foreach(p => reqBody.set(p._1, p._2))

    Mono.fromFuture(req.sendFormFuture(reqBody))
      .map(bodyToJson)
      .map(handleErrorResp)
  }

  def bodyToJson(resp: HttpResponse[Buffer]): Json = {
    val bodyOpt = resp.bodyAsString()
    if (bodyOpt.isEmpty) throw new NoSuchElementException(s"Body response is empty")

    parse(bodyOpt.get) match {
      case Left(failure) => throw failure
      case Right(json) => json
    }
  }

  def handleErrorResp(json: Json): Json = {
    root.error.string.getOption(json) match {
      case Some(errorMsg) => throw new Exception(errorMsg)
      case None => json
    }
  }

  private def mapJsonToObject[T](implicit decoder: Decoder[T]): Json => T = (json: Json) => json.as[T] match {
    case Left(err) => throw err
    case Right(value) => value
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

  private def isEqual(json: Json, commandChannel: Command.Channel): Boolean = {
    root(0).int.getOption(json).contains(commandChannel)
  }
}

object PoloniexApi {
  trait PoloniexApiKeyTag
  trait PoloniexApiSecretTag

  type Market = String // BTC_LTC
  type Currency = String // BTC

  case class Ticker (
    id: Long,
    last: BigDecimal,
    lowestAsk: BigDecimal,
    highestBid: BigDecimal,
    percentChange: BigDecimal,
    baseVolume: BigDecimal,
    quoteVolume: BigDecimal,
    isFrozen: Int, // TODO: to boolean
    high24hr: BigDecimal,
    low24hr: BigDecimal,
  )

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

  case class _24HourExchangeVolume(
    time: LocalDateTime,
    usersOnline: Int,
    baseCurrency24HVolume: Map[String, BigDecimal],
  )

  case class OrderBook(
    asks: Array[Array[BigDecimal]],
    bids: Array[Array[BigDecimal]],
    isFrozen: Int, // TODO: to boolean
    seq: BigDecimal,
  )

  case class CompleteBalance(available: BigDecimal, onOrders: BigDecimal, btcValue: BigDecimal)

  case class OpenOrder(
    orderNumber: String,
    `type`: String, // sell/buy
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal,
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
    private[PoloniexApi] def map(json: Json): Seq[TickerData] = {
      json.asArray.map(_.view.drop(2).flatMap(_.asArray).map(mapTicker)).getOrElse(Seq())
    }

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
  }

  object _24HourExchangeVolume {
    private[PoloniexApi] def map(json: Json): Option[_24HourExchangeVolume] = {
      json.asArray.flatMap(_.view
        .drop(2)
        .flatMap(_.asArray)
        .filter(_.size >= 3)
        .map(arr => _24HourExchangeVolume(mapDate(arr(0)), mapInt(arr(1)), mapBaseCurrencies24HVolume(arr(2))))
        .headOption
      )
    }

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

    private[PoloniexApi] def mapBaseCurrencies24HVolume(json: Json): Map[String, BigDecimal] = {
      json
        .asObject
        .map(_.toMap.map(v => (v._1, v._2
          .asString
          .flatMap(amount => Try(BigDecimal(amount)).toOption)
          .getOrElse(BigDecimal(-1))))
        )
        .getOrElse(Map())
    }

  }

  object OrderBook {

  }

  object OpenOrder {

    object Type {
      val Sell = "sell"
      val Buy = "buy"
    }

  }

  case class TradeHistory(
    date: String, // TODO: Convert to date 2014-02-10 01:19:37
    `type`: String, // buy/sell
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal
  )

  case class ChartData (
    date: Long,
    high: BigDecimal,
    low: BigDecimal,
    open: BigDecimal,
    close: BigDecimal,
    volume: BigDecimal,
    quoteVolume: BigDecimal,
    weightedAverage: BigDecimal,
  )
  
  case class CurrencyDetails (
    id: BigDecimal,
    maxDailyWithdrawal: BigDecimal,
    txFee: BigDecimal,
    minConf: BigDecimal,
    disabled: Int // TODO: to bool
  )

  case class LoanOrder (
    offers: Array[LoanOrderDetails],
    demands: Array[LoanOrderDetails],
  )

  case class LoanOrderDetails(
    rate: BigDecimal,
    amount: BigDecimal,
    rangeMin: BigDecimal,
    rangeMax: BigDecimal,
  )

  case class NewAddressGenerated(
    success: Int,
    response: String,
  )

  case class DepositsWithdrawals(
    deposits: Array[DepositDetails],
    withdrawals: Array[WithdrawDetails],
  )

  case class DepositDetails(
    currency: Currency,
    address: String,
    amount: BigDecimal,
    confirmations: Int,
    txid: String,
    timestamp: Long,
    status: String, // COMPLETE
  )

  case class WithdrawDetails (
    withdrawalNumber: BigDecimal,
    currency: Currency,
    address: String,
    amount: BigDecimal,
    timestamp: Long,
    status: String,
    ipAddress: String,
  )

  case class TradeHistoryPrivate(
    globalTradeID: BigDecimal,
    tradeID: BigDecimal,
    date: String, // 2016-05-03 01:29:55
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal,
    fee: BigDecimal,
    orderNumber: BigDecimal,
    `type`: String, // buy
    category: String, // settlement
  )

  case class OrderTrade (
    globalTradeID: BigDecimal,
    tradeID: BigDecimal,
    currencyPair: Market,
    `type`: String, // buy,
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal,
    fee: BigDecimal,
    date: String, // 2016-03-14 01:04:36
  )

  case class AvailableAccountBalance(
    exchange: Map[Currency, BigDecimal],
    margin: Map[Currency, BigDecimal],
    lending: Map[Currency, BigDecimal],
  )

  case class MarginAccountSummary(
    totalValue: BigDecimal,
    pl: BigDecimal,
    lendingFees: BigDecimal,
    netValue: BigDecimal,
    totalBorrowedValue: BigDecimal,
    currentMargin: BigDecimal,
  )
  
  case class Buy(
    orderNumber: BigDecimal,
    resultingTrades: Array[BuyResultingTrade],
  )
  
  case class BuyResultingTrade(
      amount: BigDecimal,
      date: LocalDateTime,
      rate: BigDecimal,
      total: BigDecimal,
      tradeID: BigDecimal,
      `type`: String,
  )

  object BuyResultingTrade {
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    implicit val dateEncoder: Encoder[LocalDateTime] = Encoder.encodeString.contramap[LocalDateTime](_.format(formatter))
    implicit val dateDecoder: Decoder[LocalDateTime] = Decoder.decodeString.emap[LocalDateTime](str => {
      Either.catchNonFatal(LocalDateTime.parse(str, formatter)).leftMap(_.getMessage)
    })
  }
}