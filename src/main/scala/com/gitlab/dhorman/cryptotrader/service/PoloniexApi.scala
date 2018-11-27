package com.gitlab.dhorman.cryptotrader.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import cats.syntax.either._
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.Codecs._
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.ErrorMsgPattern._
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi._
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.exception._
import com.gitlab.dhorman.cryptotrader.util.RequestLimiter
import com.roundeights.hasher.Implicits._
import com.softwaremill.tagging._
import com.typesafe.scalalogging.Logger
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.extras._
import io.circe.generic.semiauto._
import io.circe.optics.JsonPath._
import io.circe.parser.parse
import io.circe.syntax._
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.net.ProxyType
import io.vertx.reactivex.core.http.WebSocket
import io.vertx.reactivex.core.{Vertx => VertxRx}
import io.vertx.scala.core.net.ProxyOptions
import io.vertx.scala.core.{MultiMap => MultiMapScala, Vertx => VertxScala}
import io.vertx.scala.ext.web.client.{HttpResponse, WebClient, WebClientOptions}
import reactor.core.publisher.FluxSink
import reactor.core.scala.publisher.{Flux, Mono}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import scala.util.matching.Regex


/**
  * Documentation https://poloniex.com/support/api
  */
class PoloniexApi(
  private val scalaVertx: VertxScala,
  private val poloniexApiKey: String @@ PoloniexApiKeyTag,
  private val poloniexApiSecret: String @@ PoloniexApiSecretTag,
)(implicit val ec: ExecutionContext) {
  private val logger = Logger[PoloniexApi]
  private val reqLimiter = new RequestLimiter(allowedRequests = 6, perInterval = 1.second)

  private val vertxRx = new VertxRx(scalaVertx.asJava.asInstanceOf[Vertx])

  private val PoloniexPrivatePublicHttpApiUrl = "poloniex.com"
  private val PoloniexWebSocketApiUrl = "api2.poloniex.com"

  private val httpOptions = {
    val options = WebClientOptions()
      .setUserAgentEnabled(false)
      .setKeepAlive(true)
      .setSsl(true)

    if (sys.env.get("HTTP_CERT_TRUST_ALL").isDefined) {
      options.setTrustAll(true)
    }

    val httpProxy: Option[ProxyOptions] = sys.env.get("HTTP_PROXY_ENABLED").map(_ => {
      val options = ProxyOptions()
      val host = sys.env.get("HTTP_PROXY_HOST")
      val port = sys.env.get("HTTP_PROXY_PORT").flatMap(p => Try(Integer.parseInt(p)).toOption)
      val tpe = sys.env.get("HTTP_PROXY_TYPE").map {
        case "http" => ProxyType.HTTP
        case "socks5" => ProxyType.SOCKS5
        case _ => throw new Exception("Can't recognize HTTP_PROXY_TYPE option")
      }

      if (host.isDefined) options.setHost(host.get)
      if (port.isDefined) options.setPort(port.get)
      if (tpe.isDefined) options.setType(tpe.get)

      options
    })

    if (httpProxy.isDefined) {
      options.setProxyOptions(httpProxy.get)
    }

    options
  }

  private val httpClient = vertxRx.createHttpClient(httpOptions.asJava)
  private val webclient = WebClient.create(scalaVertx, httpOptions)

  private val websocket = {
    Flux.create((sink: FluxSink[WebSocket]) => Mono.from(httpClient
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
  }

  private val websocketMessages = {
    websocket
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
  }

  /**
    * Subscribe to ticker updates for all currency pairs.
    */
  val tickerStream: Flux[TickerData] = {
    Flux.create(create(Command.Channel.TickerData))
      .map(_.as[Seq[TickerData]] match {
        case Right(value) => value
        case Left(err) => throw new Exception(err)
      })
      .flatMapIterable(arr => arr)
      .share()
  }

  val _24HourExchangeVolumeStream: Flux[_24HourExchangeVolume] = {
    Flux.create(create(Command.Channel._24HourExchangeVolume))
      .map(_24HourExchangeVolume.map)
      .filter(_.isDefined)
      .map(_.get)
      .share()
  }

  val accountNotificationStream: Flux[AccountNotification] = {
    Flux.create(create(Command.Channel.AccountNotifications, privateApi = true))
      .map(AccountNotification.map)
      .flatMapIterable(l => l)
      .share()
  }

  def orderBookStream(marketId: Int): Flux[PriceAggregatedBook] = {
    Flux.create(create(marketId))
      .scan(new PriceAggregatedBook(), (oBook: PriceAggregatedBook, json) => {
        val (_, currentOrderNumber, commandsJson) = json.as[(Int, Long, Json)].toOption.get

        if (oBook.stamp.isDefined && oBook.stamp.get + 1 != currentOrderNumber) {
          throw new Exception("Order book broken")
        } else {
          oBook.stamp = Some(currentOrderNumber)
        }

        commandsJson.asArray.get.foreach(json => {
          val msgType = root(0).string.getOption(json).get

          msgType match {
            case "i" =>
              // initial book snapshot

              val (asks, bids) = root(1).orderBook.as[(Map[BigDecimal, BigDecimal], Map[BigDecimal, BigDecimal])].getOption(json).get

              oBook.init(asks, bids)
            case "o" =>
              // book modifications
              val (_, buySell, price, quantity) = json.as[(String, Int, BigDecimal, BigDecimal)].toOption.get

              val book = buySell match {
                case 0 => oBook.asks
                case 1 => oBook.bids
                case _ => throw new Exception("Can't decode book type")
              }

              if (quantity == 0) {
                book.remove(price)
              } else {
                book.update(price, quantity)
              }
            case "t" =>
              // trades

              // Ignore trades

              // val (_, tradeId, buySell, price, quantity, timestamp) = json.as[(String, BigDecimal, Int, BigDecimal, BigDecimal, Long)].toOption.get
          }
        })

        oBook
      })
      .share()
  }

  /**
    *
    * @return Returns the ticker for all markets.
    */
  def ticker(): Mono[Map[Market, Ticker]] = {
    val command = "returnTicker"
    callPublicApi(command).map(jsonToObject[Map[Market, Ticker]])
  }

  // TODO: Incorrect json response
  /**
    *
    * @return Returns the 24-hour volume for all markets, plus totals for primary currencies.
    */
  def get24Volume(): Mono[Map[Market, Map[Currency, BigDecimal]]] = {
    val command = "return24Volume"
    callPublicApi(command).map(jsonToObject[Map[Market, Map[Currency, BigDecimal]]])
  }

  def orderBook(currencyPair: Option[String], depth: Int): Mono[Map[Market, OrderBook]] = {
    val command = "returnOrderBook"
    val params = Map("currencyPair" -> currencyPair.getOrElse("all"), "depth" -> depth.toString)
    if (currencyPair.isEmpty) {
      val jsonToObjectMapper = jsonToObject[Map[Market, OrderBook]]
      callPublicApi(command, params).map(jsonToObjectMapper)
    } else {
      val jsonToObjectMapper = jsonToObject[OrderBook]
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

    callPublicApi(command, params).map(jsonToObject[Array[TradeHistory]])
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

    callPublicApi(command, params).map(jsonToObject[Array[ChartData]])
  }

  def currencies(): Mono[Map[Currency, CurrencyDetails]] = {
    val command = "returnCurrencies"
    callPublicApi(command).map(jsonToObject[Map[Currency, CurrencyDetails]])
  }

  def loanOrders(currency: Currency): Mono[LoanOrder] = {
    val command = "returnLoanOrders"
    val params = Map("currency" -> currency)
    callPublicApi(command, params).map(jsonToObject[LoanOrder])
  }

  /**
    * Returns all of your available balances
    */
  def balances(): Mono[Map[Currency, BigDecimal]] = {
    callPrivateApi("returnBalances").map(jsonToObject[Map[Currency, BigDecimal]])
  }

  /**
    * Returns all of your balances, including available balance, balance on orders, and the estimated BTC value of your balance.
    */
  def completeBalances(): Mono[Map[Currency, CompleteBalance]] = {
    callPrivateApi("returnCompleteBalances").map(jsonToObject[Map[Currency, CompleteBalance]])
  }

  /**
    * Returns all of your deposit addresses.
    */
  def depositAddresses(): Mono[Map[Currency, String]] = {
    callPrivateApi("returnDepositAddresses").map(jsonToObject[Map[Currency, String]])
  }


  def generateNewAddress(currency: Currency): Mono[String] = {
    val command = "generateNewAddress"
    val params = Map("currency" -> currency)
    val jsonToObjectMapper = jsonToObject[NewAddressGenerated]
    callPrivateApi(command, params).map(jsonToObjectMapper).map(_.response) // TODO: check success flag
  }

  def depositsWithdrawals(fromDate: Long, toDate: Long): Mono[DepositsWithdrawals] = {
    val command = "returnDepositsWithdrawals"
    val params = Map("start" -> fromDate.toString, "end" -> toDate.toString)
    val jsonToObjectMapper = jsonToObject[DepositsWithdrawals]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  /**
    * Returns your open orders for a given market, specified by the currencyPair.
    */
  def openOrders(currencyPair: Market): Mono[List[OpenOrder]] = {
    callPrivateApi("returnOpenOrders", Map("currencyPair" -> currencyPair))
      .map(jsonToObject[List[OpenOrder]])
  }

  def allOpenOrders(): Mono[Map[Market, List[OpenOrder]]] = {
    val command = "returnOpenOrders"
    val params = Map("currencyPair" -> "all")
    val jsonToObjectMapper = jsonToObject[Map[Market, List[OpenOrder]]]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  def tradeHistory(market: Option[Market]): Mono[Map[Market, List[TradeHistoryPrivate]]] = {
    val command = "returnTradeHistory"
    val params = Map("currencyPair" -> market.getOrElse("all"))
    if (market.isEmpty) {
      val jsonToObjectMapper = jsonToObject[Map[Market, List[TradeHistoryPrivate]]]
      callPrivateApi(command, params).map(jsonToObjectMapper)
    } else {
      val jsonToObjectMapper = jsonToObject[List[TradeHistoryPrivate]]
      callPrivateApi(command, params).map(jsonToObjectMapper).map(v => Map(market.get -> v))
    }
  }

  def orderTrades(orderNumber: BigDecimal): Mono[List[OrderTrade]] = {
    val command = "returnOrderTrades"
    val params = Map("orderNumber" -> orderNumber.toString)
    val jsonToObjectMapper = jsonToObject[List[OrderTrade]]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  // TODO:
  def orderStatus(): Mono[Unit] = {
    val command = "returnOrderStatus"
    val params = Map[String, String]()
    val jsonToObjectMapper = jsonToObject[Unit]
    callPrivateApi(command, params).map(jsonToObjectMapper)
  }

  /**
    * Places a limit buy order in a given market.
    *
    * @param tpe You may optionally set "fillOrKill", "immediateOrCancel", "postOnly".
    *            A fill-or-kill order will either fill in its entirety or be completely aborted.
    *            An immediate-or-cancel order can be partially or completely filled, but any portion of the order that cannot be filled immediately will be canceled rather than left on the order book.
    *            A post-only order will only be placed if no portion of it fills immediately; this guarantees you will never pay the taker fee on any part of the order that fills.
    * @return If successful, the method will return the order number.
    */
  def buy(market: Market, price: BigDecimal, amount: BigDecimal, tpe: Option[BuyOrderType]): Mono[Buy] = {
    buySell("buy", market, price, amount, tpe)
  }

  def sell(market: Market, price: BigDecimal, amount: BigDecimal, tpe: Option[BuyOrderType]): Mono[Buy] = {
    buySell("sell", market, price, amount, tpe)
  }

  /**
    * A limit order is one of the most basic order types. It allows the trader to specify a price and amount they would like to buy or sell.
    *
    * Example: If the current market price is 250 and I want to buy lower than that at 249, then I would place a limit buy order at 249. If the market reaches 249 and a seller’s ask matches with my bid, my limit order will be executed at 249.
    */
  private def buySell(command: String, market: Market, price: BigDecimal, amount: BigDecimal, tpe: Option[BuyOrderType]): Mono[Buy] = {
    val params = Map(
      "currencyPair" -> market,
      "rate" -> price.toString,
      "amount" -> amount.toString,
    )
    val additionalParams = tpe.map(t => Map(t -> "1")).getOrElse({Map[String, String]()})
    callPrivateApi(command, params ++ additionalParams).map(jsonToObject[Buy])
  }

  def cancelOrder(orderId: BigDecimal): Mono[Boolean] = {
    val command = "cancelOrder"
    val params = Map("orderNumber" -> orderId.toString)
    callPrivateApi(command, params).map(json => {
      root.success
        .int
        .getOption(json)
        .map(_ == 1)
        .getOrElse({throw new Exception("Can't get value")})
    })
  }

  /**
    * Cancels an order and places a new one of the same type in a single atomic transaction, meaning either both operations will succeed or both will fail.
    *
    * @param orderType "postOnly" or "immediateOrCancel" may be specified for exchange orders, but will have no effect on margin orders.
    * @return
    */
  def moveOrder(orderNumber: BigDecimal, price: BigDecimal, amount: Option[BigDecimal], orderType: Option[BuyOrderType]): Mono[MoveOrderResult] = {
    val command = "moveOrder"
    val params = Map(
      "orderNumber" -> orderNumber.toString,
      "rate" -> price.toString,
    )
    val optParams = Map(
      Some("amount") -> amount,
      orderType -> Some("1"),
    )
      .view
      .filter(v => v._1.isDefined && v._2.isDefined)
      .map(v => (v._1.get.toString, v._2.get.toString))
      .toMap

    callPrivateApi(command, params ++ optParams)
      .map(convertJsonSuccessFieldFromIntToBool)
      .map(jsonToObject[MoveOrderResult])
  }


  /**
    * Immediately places a withdrawal for a given currency, with no email confirmation. In order to use this method, the withdrawal privilege must be enabled for your API key.
    *
    * @param paymentId For XMR withdrawals, you may optionally specify "paymentId"
    * @return
    */
  def withdraw(currency: Currency, amount: BigDecimal, address: String, paymentId: Option[String]): Mono[WithdrawResp] = {
    val command = "withdraw"
    val params = Map(
      "currency" -> currency.toString,
      "amount" -> amount.toString,
      "address" -> address,
    )
    val optParams = paymentId.map(v => Map("paymentId" -> v)).getOrElse({Map[String, String]()})
    callPrivateApi(command, params ++ optParams).map(jsonToObject[WithdrawResp])
  }

  /**
    * If you are enrolled in the maker-taker fee schedule, returns your current trading fees and trailing 30-day volume in BTC. This information is updated once every 24 hours.
    */
  def feeInfo(): Mono[FeeInfo] = {
    val command = "returnFeeInfo"
    callPrivateApi(command).map(jsonToObject[FeeInfo])
  }

  def availableAccountBalances(): Mono[AvailableAccountBalance] = {
    val command = "returnAvailableAccountBalances"
    val jsonToObjectMapper = jsonToObject[AvailableAccountBalance]
    callPrivateApi(command).map(jsonToObjectMapper)
  }

  def tradableBalances(): Mono[Map[Market, Map[Currency, BigDecimal]]] = {
    val command = "returnTradableBalances"
    val jsonToObjectMapper = jsonToObject[Map[Market, Map[Currency, BigDecimal]]]
    callPrivateApi(command).map(jsonToObjectMapper)
  }

  /**
    * Transfers funds from one account to another (e.g. from your exchange account to your margin account).
    */
  def transferBalance(currency: Currency, amount: BigDecimal, fromAccount: AccountType, toAccount: AccountType): Mono[TransferBalanceResp] = {
    val command = "transferBalance"
    val params = Map(
      "currency" -> currency,
      "amount" -> amount.toString,
      "fromAccount" -> AccountType.toApiString(fromAccount),
      "toAccount" -> AccountType.toApiString(toAccount),
    )
    callPrivateApi(command, params)
      .map(convertJsonSuccessFieldFromIntToBool)
      .map(jsonToObject[TransferBalanceResp])
  }

  /**
    * Returns a summary of your entire margin account. This is the same information you will find in the Margin Account section of the Margin Trading page, under the Markets list.
    */
  def marginAccountSummary(): Mono[MarginAccountSummary] = {
    val command = "returnMarginAccountSummary"
    callPrivateApi(command).map(jsonToObject[MarginAccountSummary])
  }

  def marginBuy(market: Market, price: BigDecimal, amount: BigDecimal, lendingRate: Option[BigDecimal]): Mono[MarginBuySell] = {
    marginBuySell("marginBuy", market, price, amount, lendingRate)
  }

  def marginSell(market: Market, price: BigDecimal, amount: BigDecimal, lendingRate: Option[BigDecimal]): Mono[MarginBuySell] = {
    marginBuySell("marginSell", market, price, amount, lendingRate)
  }

  /**
    * Places a margin buy/sell order in a given market.
    *
    * @param lendingRate You may optionally specify a maximum lending rate using the "lendingRate" parameter.
    * @return If successful, the method will return the order number and any trades immediately resulting from your order.
    */
  private def marginBuySell(command: String, market: Market, price: BigDecimal, amount: BigDecimal, lendingRate: Option[BigDecimal]): Mono[MarginBuySell] = {
    val params = Map(
      "currencyPair" -> market,
      "rate" -> price.toString,
      "amount" -> amount.toString,
    )
    val optParams = lendingRate.map(rate => Map("lendingRate" -> rate.toString)).getOrElse({Map[String, String]()})
    callPrivateApi(command, params ++ optParams)
      .map(convertJsonSuccessFieldFromIntToBool)
      .map(jsonToObject[MarginBuySell])
  }

  /**
    * Returns information about your margin position in a given market.
    * If you have no margin position in the specified market, "type" will be set to "none". "liquidationPrice" is an estimate, and does not necessarily represent the price at which an actual forced liquidation will occur. If you have no liquidation price, the value will be -1.
    */
  // TODO: Test for market = None
  def marginPosition(market: Option[Market] = None): Mono[MarginPosition] = {
    val command = "getMarginPosition"
    val params = Map(
      "currencyPair" -> market.getOrElse("all")
    )
    callPrivateApi(command, params).map(jsonToObject[MarginPosition])
  }

  def closeMarginPosition(market: Market): Mono[CloseMarginPosition] = {
    val command = "closeMarginPosition"
    val params = Map("currencyPair" -> market)
    callPrivateApi(command, params)
      .map(convertJsonSuccessFieldFromIntToBool)
      .map(jsonToObject[CloseMarginPosition])
  }

  /**
    * Creates a loan offer for a given currency.
    */
  def createLoanOffer(currency: Currency, amount: BigDecimal, duration: Long, autoRenew: Boolean, lendingRate: BigDecimal): Mono[CreateLoanOffer] = {
    val command = "createLoanOffer"
    val params = Map(
      "currency" -> currency,
      "amount" -> amount.toString,
      "duration" -> duration.toString,
      "autoRenew" -> (if (autoRenew) "1" else "0"),
      "lendingRate" -> lendingRate.toString,
    )
    callPrivateApi(command, params)
      .map(convertJsonSuccessFieldFromIntToBool)
      .map(jsonToObject[CreateLoanOffer])
  }

  /**
    * Cancels a loan offer
    */
  def cancelLoanOffer(orderNumber: BigDecimal): Mono[CancelLoanOffer] = {
    val command = "cancelLoanOffer"
    val params = Map("orderNumber" -> orderNumber.toString)
    callPrivateApi(command, params)
      .map(convertJsonSuccessFieldFromIntToBool)
      .map(jsonToObject[CancelLoanOffer])
  }

  /**
    * Returns your open loan offers for each currency.
    */
  def openLoanOffers(): Mono[Map[Currency, OpenLoanOffer]] = {
    val command = "returnOpenLoanOffers"
    callPrivateApi(command).map(jsonToObject[Map[Currency, OpenLoanOffer]])
  }

  /**
    * Returns your active loans for each currency.
    */
  def activeLoans(): Mono[ActiveLoan] = {
    val command = "returnActiveLoans"
    callPrivateApi(command).map(jsonToObject[ActiveLoan])
  }

  def lendingHistory(fromTime: Long, toTime: Long, limit: Option[Long]): Mono[Array[LendingHistory]] = {
    val command = "returnLendingHistory"
    val params = Map(
      "start" -> fromTime.toString,
      "end" -> toTime.toString,
    )
    val optParams = limit.map(l => Map("limit" -> l.toString)).getOrElse({Map()})
    callPrivateApi(command, params ++ optParams).map(jsonToObject[Array[LendingHistory]])
  }

  def toggleAutoRenew(orderNumber: BigDecimal): Mono[ToggleAutoRenew] = {
    val command = "toggleAutoRenew"
    val params = Map("orderNumber" -> orderNumber.toString)
    callPrivateApi(command, params).map(jsonToObject[ToggleAutoRenew])
  }


  private def callPublicApi(command: String, queryParams: Map[String, String] = Map()): Mono[Json] = {
    val qParams = (Map("command" -> command) ++ queryParams).view.map(p => s"${p._1}=${p._2}").mkString("&")
    Mono.defer(() => Mono.fromFuture(
      webclient
        .get(443, PoloniexPrivatePublicHttpApiUrl, s"/public?$qParams")
        .ssl(true)
        .sendFuture()
    )).transform(handleApiCallResp)
  }

  private def callPrivateApi(methodName: String, postArgs: Map[String, String] = Map()): Mono[Json] = {
    Mono.defer(() => {
      val postParamsPrivate = Map("command" -> methodName, "nonce" -> System.currentTimeMillis().toString)
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
    }).transform(handleApiCallResp)
  }

  private def convertJsonSuccessFieldFromIntToBool: Json => Json = _.hcursor
    .downField("success")
    .withFocus(_.withNumber(_.toInt.exists(_ == 1).asJson))
    .top
    .getOrElse({throw new Exception("Can't convert success field from int to boolean")})

  private def handleApiCallResp(resp: Mono[HttpResponse[Buffer]]): Mono[Json] = {
    resp
      .map(bodyToJson)
      .map(handleErrorResp)
      // TODO: Retry when nonce or api limit error
      .delaySubscription(Mono.defer(() => Mono.delay(reqLimiter.get()).onErrorReturn(0)))
  }

  private def bodyToJson(resp: HttpResponse[Buffer]): Json = {
    val bodyOpt = resp.bodyAsString()
    if (bodyOpt.isEmpty) throw new NoSuchElementException(s"Body response is empty")

    parse(bodyOpt.get) match {
      case Left(failure) => throw failure
      case Right(json) => json
    }
  }

  private def handleErrorResp(json: Json): Json = {
    root.error.string.getOption(json) match {
      case Some(errorMsg) => errorMsg match {
        case IncorrectNonceMsg(providedNonce, requiredNonce) =>
          throw IncorrectNonceException(providedNonce.toLong, requiredNonce.toLong)(errorMsg)
        case ApiCallLimit(countPerSecStr) =>
          throw ApiCallLimitException(countPerSecStr.toInt)(errorMsg)
        case _ =>
          throw new Exception(errorMsg)
      }
      case None => json
    }
  }

  private def jsonToObject[T](implicit decoder: Decoder[T]): Json => T = (json: Json) => json.as[T] match {
    case Left(err) => throw err
    case Right(value) => value
  }

  private def create[T](channel: Command.Channel, privateApi: Boolean = false): FluxSink[Json] => Unit = (sink: FluxSink[Json]) => {
    // Subscribe to ticker stream
    websocket.take(1).subscribe(socket => {
      val jsonStr = if (privateApi) {
        val payload = s"nonce=${System.currentTimeMillis()}"
        val sign = payload.hmac(poloniexApiSecret).sha512
        PrivateCommand(Command.Type.Subscribe, channel, poloniexApiKey, payload, sign).asJson.noSpaces
      } else {
        Command(Command.Type.Subscribe, channel).asJson.noSpaces
      }
      socket.writeTextMessage(jsonStr)
      logger.info(s"Subscribe to $channel channel")
    }, err => {
      sink.error(err)
    })

    // Receive data
    val messagesSubscription = websocketMessages
      .filter(isEqual(_, channel))
      .subscribe(json => {
        sink.next(json)
      }, err => {
        sink.error(err)
      }, () => {
        sink.complete()
      })

    // Unsubscribe from stream
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

  case class Ticker(
    id: Long,
    last: BigDecimal,
    lowestAsk: BigDecimal,
    highestBid: BigDecimal,
    percentChange: BigDecimal,
    baseVolume: BigDecimal,
    quoteVolume: BigDecimal,
    isFrozen: Boolean,
    high24hr: BigDecimal,
    low24hr: BigDecimal,
  )

  object Ticker {
    implicit val encoder: Encoder[Ticker] = deriveEncoder
    implicit val decoder: Decoder[Ticker] = deriveDecoder
  }

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

  case class PrivateCommand(
    command: Command.Type,
    channel: Command.Channel,
    key: String, // API key
    payload: String, // nonce=<epoch ms>
    sign: String,
  )

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
    private type TickerDataTuple = (Int, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Boolean, BigDecimal, BigDecimal)

    private[PoloniexApi] implicit val decoder: Decoder[Seq[TickerData]] = Decoder.decodeJson.emap[Seq[TickerData]](arr0 => {
      Either.fromOption(arr0.asArray, "Not an array")
        .map(_.view.drop(2).map(_.as[TickerDataTuple].map((TickerData.apply _).tupled(_)).leftMap(_.getMessage)))
        .flatMap(_.foldRight(Right(Nil): Either[String, List[TickerData]])((elem, acc) => acc.right.flatMap(l => elem.right.map(_ :: l))))
    })
  }

  case class _24HourExchangeVolume(
    time: LocalDateTime,
    usersOnline: Int,
    baseCurrency24HVolume: Map[Currency, BigDecimal],
  )

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
      json.asString.flatMap(dateStr => Try(LocalDateTime.parse(dateStr, localDateTimeHourMinuteFormatter)).toOption).getOrElse(LocalDateTime.now())
    }

    private[PoloniexApi] def mapInt(json: Json): Int = {
      json.asNumber.flatMap(_.toInt).getOrElse(-1)
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

  case class OrderBook(
    asks: Array[Array[BigDecimal]],
    bids: Array[Array[BigDecimal]],
    isFrozen: Boolean,
    seq: BigDecimal,
  )

  object OrderBook {
    implicit val encoder: Encoder[OrderBook] = deriveEncoder
    implicit val decoder: Decoder[OrderBook] = deriveDecoder
  }

  class PriceAggregatedBook() {
    var stamp: Option[Long] = None
    val asks = new mutable.TreeMap[BigDecimal, BigDecimal]()
    val bids = new mutable.TreeMap[BigDecimal, BigDecimal]()(implicitly[Ordering[BigDecimal]].reverse)

    def init(_asks: Map[BigDecimal, BigDecimal], _bids: Map[BigDecimal, BigDecimal]): Unit = {
      _asks.foreach(ask => asks += ask)
      _bids.foreach(bid => bids += bid)
    }
  }

  case class CompleteBalance(available: BigDecimal, onOrders: BigDecimal, btcValue: BigDecimal)

  case class OpenOrder(
    orderNumber: String,
    `type`: OrderType,
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal,
  )

  object OpenOrder {
    implicit val decoder: Decoder[OpenOrder] = deriveDecoder
  }

  case class TradeHistory(
    date: LocalDateTime,
    `type`: OrderType,
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal
  )

  object TradeHistory {
    implicit val decoder: Decoder[TradeHistory] = deriveDecoder
  }

  case class ChartData(
    date: Long,
    high: BigDecimal,
    low: BigDecimal,
    open: BigDecimal,
    close: BigDecimal,
    volume: BigDecimal,
    quoteVolume: BigDecimal,
    weightedAverage: BigDecimal,
  )

  case class CurrencyDetails(
    id: Long,
    name: String,
    txFee: BigDecimal,
    minConf: BigDecimal,
    depositAddress: Option[String],
    disabled: Boolean,
    delisted: Boolean,
    frozen: Boolean,
  )

  object CurrencyDetails {
    implicit val encoder: Encoder[CurrencyDetails] = deriveEncoder
    implicit val decoder: Decoder[CurrencyDetails] = deriveDecoder
  }

  case class LoanOrder(
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
    success: Boolean,
    response: String,
  )

  object NewAddressGenerated {
    implicit val encoder: Encoder[NewAddressGenerated] = deriveEncoder
    implicit val decoder: Decoder[NewAddressGenerated] = deriveDecoder
  }

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
    status: String, // TODO: COMPLETE
  )

  object DepositDetails {
    implicit val encoder: Encoder[DepositDetails] = deriveEncoder
    implicit val decoder: Decoder[DepositDetails] = deriveDecoder
  }

  case class WithdrawDetails(
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
    date: LocalDateTime,
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal,
    fee: BigDecimal,
    orderNumber: BigDecimal,
    `type`: OrderType,
    category: String, // TODO: settlement
  )

  object TradeHistoryPrivate {
    implicit val decoder: Decoder[TradeHistoryPrivate] = deriveDecoder
  }

  case class OrderTrade(
    globalTradeID: BigDecimal,
    tradeID: BigDecimal,
    currencyPair: Market,
    `type`: OrderType,
    rate: BigDecimal,
    amount: BigDecimal,
    total: BigDecimal,
    fee: BigDecimal,
    date: LocalDateTime,
  )

  object OrderTrade {
    implicit val decoder: Decoder[OrderTrade] = deriveDecoder
  }

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
    @JsonKey("tradeID") tradeId: BigDecimal,
    `type`: OrderType,
  )

  object BuyResultingTrade {
    implicit val decoder: Decoder[BuyResultingTrade] = deriveDecoder
  }

  // TODO: Use enum

  type BuyOrderType = String

  object BuyOrderType {
    // A "fill or kill" order is a limit order that must be filled immediately in its entirety or it is canceled (killed). The purpose of a fill or kill order is to ensure that a position is entered instantly and at a specific price.
    val FillOrKill: BuyOrderType = "fillOrKill"

    // An Immediate Or Cancel (IOC) order requires all or part of the order to be executed immediately, and any unfilled parts of the order are canceled. Partial fills are accepted with this type of order duration, unlike a fill-or-kill order, which must be filled immediately in its entirety or be canceled.
    val ImmediateOrCancel: BuyOrderType = "immediateOrCancel"

    // https://support.bitfinex.com/hc/en-us/articles/115003507365-Post-Only-Limit-Order-Option
    // The post-only limit order option ensures the limit order will be added to the order book and not match with a pre-existing order. If your order would cause a match with a pre-existing order, your post-only limit order will be canceled. This ensures that you will pay the maker fee and not the taker fee. Visit the fees page for more information.
    val PostOnly: BuyOrderType = "postOnly"
  }

  case class MoveOrderResult(
    success: Boolean,
    orderNumber: BigDecimal,
    resultingTrades: Map[Market, Array[BuyResultingTrade]],
  )

  case class WithdrawResp(
    response: String,
  )

  case class FeeInfo(
    makerFee: BigDecimal,
    takerFee: BigDecimal,
    thirtyDayVolume: BigDecimal,
    nextTier: BigDecimal,
  )

  case class TransferBalanceResp(
    success: Boolean,
    message: String,
  )

  sealed trait AccountType

  object AccountType {

    case object Exchange extends AccountType

    case object Margin extends AccountType

    private[PoloniexApi] def toApiString(accountType: AccountType): String = accountType match {
      case Exchange => "exchange"
      case Margin => "margin"
    }
  }

  case class MarginBuySell(
    success: Boolean,
    message: String,
    orderNumber: BigDecimal,
    resultingTrades: Map[Market, Array[BuyResultingTrade]],
  )

  case class MarginPosition(
    amount: BigDecimal,
    total: BigDecimal,
    basePrice: BigDecimal,
    liquidationPrice: BigDecimal,
    pl: BigDecimal,
    lendingFees: BigDecimal,
    `type`: String,
  )

  case class CloseMarginPosition(
    success: Boolean,
    message: String,
    resultingTrades: Map[Market, Array[BuyResultingTrade]]
  )

  case class CreateLoanOffer(
    success: Boolean,
    message: String,
    @JsonKey("orderID") orderId: BigDecimal,
  )

  case class CancelLoanOffer(
    success: Boolean,
    message: String,
  )

  case class ToggleAutoRenew(
    success: Int,
    message: String,
  )

  case class LendingHistory(
    id: BigDecimal,
    currency: Currency,
    rate: BigDecimal,
    amount: BigDecimal,
    duration: BigDecimal,
    interest: BigDecimal,
    fee: BigDecimal,
    earned: BigDecimal,
    open: LocalDateTime,
    close: LocalDateTime,
  )

  object LendingHistory {
    implicit val encoder: Encoder[LendingHistory] = deriveEncoder
    implicit val decoder: Decoder[LendingHistory] = deriveDecoder
  }

  case class ActiveLoan(
    provided: Array[ActiveLoanDetail],
    used: Array[ActiveLoanDetail],
  )

  case class ActiveLoanDetail(
    id: BigInt,
    currency: Currency,
    rate: BigDecimal,
    amount: BigDecimal,
    range: Int,
    autoRenew: Int,
    date: LocalDateTime,
    fees: BigDecimal,
  )

  object ActiveLoanDetail {
    implicit val encoder: Encoder[ActiveLoanDetail] = deriveEncoder
    implicit val decoder: Decoder[ActiveLoanDetail] = deriveDecoder
  }

  case class OpenLoanOffer(
    id: BigInt,
    rate: BigDecimal,
    amount: BigDecimal,
    duration: Long,
    autoRenew: Boolean,
    date: LocalDateTime,
  )

  object OpenLoanOffer {
    implicit val encoder: Encoder[OpenLoanOffer] = deriveEncoder
    implicit val decoder: Decoder[OpenLoanOffer] = deriveDecoder
  }

  sealed trait AccountNotification

  object AccountNotification {
    private[PoloniexApi] def map(json: Json): Seq[AccountNotification] = {
      val respArr = json.asArray.get

      if (respArr.length == 2) {
        if (respArr(1).as[Int].toOption.get == 0) {
          throw new Exception("Bad acknowledgement of the subscription")
        } else {
          Seq()
        }
      } else {
        respArr(2).asArray.get.map(jsonValue => {
          val msgType = root(0).string.getOption(jsonValue).get

          val msg: Either[DecodingFailure, AccountNotification] = msgType match {
            case "b" => jsonValue.as[BalanceUpdate]
            case "n" => jsonValue.as[LimitOrderCreated]
            case "o" => jsonValue.as[OrderUpdate]
            case "t" => jsonValue.as[TradeNotification]
            case _ => throw new Exception("Message not recognized")
          }

          msg match {
            case Left(err) => throw err
            case Right(value) => value
          }
        })
      }
    }
  }

  final case class BalanceUpdate(currencyId: Int, walletType: WalletType, amount: BigDecimal) extends AccountNotification

  object BalanceUpdate {
    private[PoloniexApi] type ArrayDecoder = (String, Int, WalletType, BigDecimal)

    private[PoloniexApi] implicit val decoder: Decoder[BalanceUpdate] = (c: HCursor) => {
      c.as[ArrayDecoder].map(b => BalanceUpdate(b._2, b._3, b._4))
    }
  }

  final case class LimitOrderCreated(
    marketId: Int,
    orderNumber: Long,
    orderType: OrderType,
    rate: BigDecimal,
    amount: BigDecimal,
    date: LocalDateTime,
  ) extends AccountNotification

  object LimitOrderCreated {
    private[PoloniexApi] type ArrayDecoder = (String, Int, Long, OrderType, BigDecimal, BigDecimal, LocalDateTime)

    private[PoloniexApi] implicit val decoder: Decoder[LimitOrderCreated] = (c: HCursor) => {
      c.as[ArrayDecoder].map(n => LimitOrderCreated(n._2, n._3, n._4, n._5, n._6, n._7))
    }
  }

  final case class OrderUpdate(orderNumber: Long, newAmount: BigDecimal) extends AccountNotification

  object OrderUpdate {
    private[PoloniexApi] type ArrayDecoder = (String, Long, BigDecimal)

    private[PoloniexApi] implicit val decoder: Decoder[OrderUpdate] = (c: HCursor) => {
      c.as[ArrayDecoder].map(o => OrderUpdate(o._2, o._3))
    }
  }

  final case class TradeNotification(
    tradeId: Long,
    rate: BigDecimal,
    amount: BigDecimal,
    feeMultiplier: BigDecimal,
    fundingType: FundingType,
    orderNumber: Long,
  ) extends AccountNotification

  object TradeNotification {
    private[PoloniexApi] type ArrayDecoder = (String, Long, BigDecimal, BigDecimal, BigDecimal, FundingType, Long)

    private[PoloniexApi] implicit val decoder: Decoder[TradeNotification] = (c: HCursor) => {
      c.as[ArrayDecoder].map(o => TradeNotification(o._2, o._3, o._4, o._5, o._6, o._7))
    }
  }

  type WalletType = WalletType.Value
  object WalletType extends Enumeration {
    val Exchange: WalletType = Value
    val Margin: WalletType = Value
    val Lending: WalletType = Value

    private[PoloniexApi] implicit val decoder: Decoder[WalletType] = Decoder.decodeString.emap {
      case "e" => Right(WalletType.Exchange)
      case "m" => Right(WalletType.Margin)
      case "l" => Right(WalletType.Lending)
      case _ => Left("Not recognized wallet type. New wallet added ?")
    }
  }

  type OrderType = OrderType.Value
  object OrderType extends Enumeration {
    val Sell: OrderType = Value
    val Buy: OrderType = Value

    private[PoloniexApi] implicit val decoder: Decoder[OrderType] = Decoder.decodeJson.emap(json => {
      if (json.isString) {
        json.asString.get match {
          case "sell" => Right(OrderType.Sell)
          case "buy" => Right(OrderType.Buy)
          case str => Left(s"""Not recognized string "$str" """)
        }
      } else if (json.isNumber) {
        json.asNumber.get.toInt.get match {
          case 0 => Right(OrderType.Sell)
          case 1 => Right(OrderType.Buy)
          case int => Left(s"""Not recognized integer "$int" """)
        }
      } else {
        Left("Can't parse OrderType")
      }
    })
  }

  type FundingType = FundingType.Value
  object FundingType extends Enumeration {
    val ExchangeWallet: FundingType = Value
    val BorrowedFunds: FundingType = Value
    val MarginFunds: FundingType = Value
    val LendingFunds: FundingType = Value

    private[PoloniexApi] implicit val decoder: Decoder[FundingType] = Decoder.decodeInt.map {
      case 0 => FundingType.ExchangeWallet
      case 1 => FundingType.BorrowedFunds
      case 2 => FundingType.MarginFunds
      case 3 => FundingType.LendingFunds
    }
  }


  object ErrorMsgPattern {
    val IncorrectNonceMsg: Regex = """Nonce must be greater than (\d+)\. You provided (\d+)\.""".r
    val ApiCallLimit: Regex = """Please do not make more than (\d+) API calls per second\.""".r
  }

  object exception {
    case class IncorrectNonceException(providedNonce: Long, requiredNonce: Long)(originalMsg: String) extends Throwable(originalMsg, null, true, false)
    case class ApiCallLimitException(maxRequestPerSecond: Int)(originalMsg: String) extends Throwable(originalMsg, null, true, false)
  }

  object Codecs {
    val localDateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val localDateTimeHourMinuteFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

    implicit val boolDecoder: Decoder[Boolean] = Decoder.decodeJson.emap[Boolean](json => {
      if (json.isNumber) {
        Right(json.asNumber.get.toInt.get == 1)
      } else if (json.isBoolean) {
        Right(json.asBoolean.get)
      } else {
        Left("Can't parse boolean")
      }
    })

    implicit val localDateTimeEncoder: Encoder[LocalDateTime] = Encoder.encodeString.contramap[LocalDateTime](_.format(localDateTimeFormatter))
    implicit val localDateTimeDecoder: Decoder[LocalDateTime] = Decoder.decodeString.emap[LocalDateTime](str => {
      Either.catchNonFatal(LocalDateTime.parse(str, localDateTimeFormatter)).leftMap(_.getMessage)
    })

    implicit val bigDecimalMapKeyDecoder: KeyDecoder[BigDecimal] = KeyDecoder.decodeKeyString.map(str => BigDecimal(str))
    implicit val bigDecimalMapKeyEncoder: KeyEncoder[BigDecimal] = KeyEncoder.encodeKeyString.contramap(_.toString)
  }
}