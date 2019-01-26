package com.gitlab.dhorman.cryptotrader.trader

import java.time.{Instant, LocalDateTime}

import com.gitlab.dhorman.cryptotrader.core._
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi._
import com.gitlab.dhorman.cryptotrader.trader.Trader.{MarketPathGenerator, OrderPlan}
import com.typesafe.scalalogging.Logger
import reactor.core.publisher.{FluxSink, ReplayProcessor}
import reactor.core.scala.publisher.{Flux, FluxProcessor}

import scala.collection.mutable

class Trader(private val poloniexApi: PoloniexApi) {
  private val logger = Logger[Trader]

  private object trigger {
    type Trigger = FluxProcessor[Unit, Unit]

    private def createTrigger(): Trigger = {
      val p = ReplayProcessor.cacheLast[Unit]()
      FluxProcessor.wrap(p, p)
    }

    val currencies: Trigger = createTrigger()
    val ticker: Trigger = createTrigger()
    val balances: Trigger = createTrigger()
    val openOrders: Trigger = createTrigger()
  }

  private object raw {
    private def wrap[T](triggerStream: trigger.Trigger, stream: Flux[T]): Flux[T] = {
      triggerStream.startWith(()).switchMap(_ => stream.take(1)).replay(1).refCount()
    }

    val currencies: Flux[Map[Currency, CurrencyDetails]] = {
      val callApiStream = poloniexApi.currencies().flux().doOnNext(curr => {
        logger.info("All currencies fetched")

        logger.whenDebugEnabled {
          logger.debug(curr.toString)
        }
      })
      wrap(trigger.currencies, callApiStream)
    }

    val ticker: Flux[Map[Market, Ticker]] = {
      val callApiStream = poloniexApi.ticker().flux().doOnNext(tickers => {
        logger.info("All tickers fetched")

        logger.whenDebugEnabled {
          logger.debug(tickers.toString)
        }
      })
      wrap(trigger.ticker, callApiStream)
    }

    val balances: Flux[Map[Currency, BigDecimal]] = {
      val callApiStream = poloniexApi.balances().flux().doOnNext(balances => {
        logger.info("All available balances fetched")
        logger.whenDebugEnabled {
          logger.debug(balances.toString)
        }
      })
      wrap(trigger.balances, callApiStream)
    }

    val openOrders: Flux[Map[Market, Set[OpenOrder]]] = {
      val callApiStream = poloniexApi.allOpenOrders().flux().doOnNext(orders => {
        logger.info("All open orders fetched")

        logger.whenDebugEnabled {
          logger.debug(orders.toString)
        }
      })
      wrap(trigger.openOrders, callApiStream)
    }

    val accountNotifications: Flux[AccountNotification] = poloniexApi.accountNotificationStream

    val tickerStream: Flux[Ticker] = poloniexApi.tickerStream
  }

  object data {
    type MarketIntMap = mutable.Map[MarketId, Market]
    type MarketStringMap = mutable.Map[Market, MarketId]
    type MarketData = (MarketIntMap, MarketStringMap)
    type OrderBookData = (Market, MarketId, PriceAggregatedBook, OrderBookNotification)
    type OrderBookDataMap = Map[MarketId, Flux[OrderBookData]]

    val currencies: Flux[(Map[Currency, CurrencyDetails], Map[Int, Currency])] = {
      raw.currencies.map(curr => {
        (curr, curr.map(kv => (kv._2.id, kv._1)))
      }).replay(1).refCount()
    }

    val balances: Flux[mutable.Map[Currency, BigDecimal]] = {
      val balancesStream = raw.balances.map(b => mutable.Map(b.toSeq: _*)).replay(1).refCount()
      val balanceChangesStream = raw.accountNotifications.filter(_.isInstanceOf[BalanceUpdate]).map(_.asInstanceOf[BalanceUpdate])

      val updatedBalance = balanceChangesStream.withLatestFrom(balancesStream, (n, b: mutable.Map[Currency, BigDecimal]) => (n, b)).concatMap { case (balanceUpdate, allBalances) => currencies.take(1).map(curr => {
        logger.info(s"Account info: $balanceUpdate")

        if (balanceUpdate.walletType == WalletType.Exchange) {
          val dataOpt = curr._2.get(balanceUpdate.currencyId).flatMap(currencyId => {
            allBalances.get(currencyId).map(balance => (currencyId, balance))
          })

          if (dataOpt.isDefined) {
            val (currencyId, balance) = dataOpt.get
            val newBalance = balance + balanceUpdate.amount
            allBalances(currencyId) = newBalance
          } else {
            logger.warn("Balances and currencies can be not in sync. Fetch new balances and currencies.")
            trigger.currencies.onNext(())
            trigger.balances.onNext(())
          }
        }

        allBalances
      })}

      Flux.merge(balancesStream, updatedBalance).replay(1).refCount()
    }

    val markets: Flux[MarketData] = {
      raw.ticker.map(tickers => {
        val marketIntStringMap: MarketIntMap = mutable.Map()
        val marketStringIntMap: MarketStringMap = mutable.Map()

        // TODO: Review frozen filter
        tickers.view.filter(!_._2.isFrozen).foreach(tick => {
          marketIntStringMap.put(tick._2.id, tick._1)
          marketStringIntMap.put(tick._1, tick._2.id)
        })

        (marketIntStringMap, marketStringIntMap)
      }).doOnNext(markets => {
        logger.info("Markets fetched")
        logger.whenDebugEnabled {
          logger.debug(markets.toString)
        }
      }).cache(1)
    }

    val tradesStat: Flux[mutable.Map[MarketId, Flux[TradeStat]]] = {
      import Trader.TradeStatModels._

      val BufferLimit = 100

      markets map {case (_, marketInfo) =>
        marketInfo map {case (market, marketId) =>
          val initialTrades: Flux[Array[TradeHistory]] = poloniexApi.tradeHistoryPublic(market).retry().flux() // TODO: Specify backoff

          val tradesStream = orderBooks.map(_(marketId))
            .switchMap(identity)
            .map(_._4)
            .filter(_.isInstanceOf[OrderBookTrade])
            .map(_.asInstanceOf[OrderBookTrade])

          val initialTrades0 = initialTrades.map { allTrades =>
            var sellTrades = Vector[SimpleTrade]()
            var buyTrades = Vector[SimpleTrade]()

            for (trade <- allTrades) {
              val trade0 = SimpleTrade(trade.price, trade.amount, trade.date.toInstant)

              if (trade.tpe == OrderType.Sell) {
                sellTrades = sellTrades :+ trade0
              } else {
                buyTrades = buyTrades :+ trade0
              }
            }

            if (sellTrades.length > BufferLimit) sellTrades = sellTrades.dropRight(sellTrades.length - BufferLimit)
            if (buyTrades.length > BufferLimit) buyTrades = buyTrades.dropRight(buyTrades.length - BufferLimit)

            Trade0(sellTrades, buyTrades)
          }

          val trades1 = initialTrades0.map { allTrades =>
            Trade1(
              sellOld = allTrades.sell,
              sellNew = allTrades.sell,
              buyOld = allTrades.buy,
              buyNew = allTrades.buy,
            )
          }.switchMap { initTrade1 => tradesStream.scan(initTrade1, (trade1: Trade1, bookTrade: OrderBookTrade) => {
            val newTrade = SimpleTrade(bookTrade.price, bookTrade.amount, bookTrade.timestamp)
            var sellOld: Vector[SimpleTrade] = null
            var sellNew: Vector[SimpleTrade] = null
            var buyOld: Vector[SimpleTrade] = null
            var buyNew: Vector[SimpleTrade] = null

            if (bookTrade.orderType == OrderType.Sell) {
              sellOld = trade1.sellNew
              sellNew = Trade1.newTrades(newTrade, trade1.sellNew, BufferLimit)
              buyOld = trade1.buyOld
              buyNew = trade1.buyNew
            } else {
              buyOld = trade1.buyNew
              buyNew = Trade1.newTrades(newTrade, trade1.buyNew, BufferLimit)
              sellOld = trade1.sellOld
              sellNew = trade1.sellNew
            }

            Trade1(sellOld, sellNew, buyOld, buyNew)
          })}

          val trades2 = trades1.scan(Trade2(null, null), (trade2: Trade2, trade1: Trade1) => {
            val a = trade1.sellOld eq trade1.sellNew
            val b = trade1.buyOld eq trade1.buyNew
            val na = trade1.sellOld ne trade1.sellNew
            val nb = trade1.buyOld ne trade1.buyNew

            var sell: Trade2State = null
            var buy: Trade2State = null

            if (na && b) {
              sell = Trade2State.calc(trade2.sell, trade1.sellOld, trade1.sellNew)
              buy = trade2.buy
            } else if (a && nb) {
              sell = trade2.sell
              buy = Trade2State.calc(trade2.buy, trade1.buyOld, trade1.buyNew)
            } else if (a && b) {
              sell = Trade2State.calcFull(trade1.sellNew)
              buy = Trade2State.calcFull(trade1.buyNew)
            } else {
              sell = Trade2State.calc(trade2.sell, trade1.sellOld, trade1.sellNew)
              buy = Trade2State.calc(trade2.buy, trade1.buyOld, trade1.buyNew)
            }

            Trade2(sell, buy)
          }).skip(1)

          val statStream = trades2.map { state =>
            TradeStat(
              sell = Trade2State.map(state.sell),
              buy = Trade2State.map(state.buy),
            )
          }.replay(1).refCount()

          (marketId, statStream)
        }
      }
    }

    val openOrders: Flux[mutable.Set[Trader.OpenOrder]] = {
      val initialOrdersStream: Flux[mutable.Set[Trader.OpenOrder]] = raw.openOrders.map(orders => {
        orders.flatMap(kv => kv._2.view.map(o => new Trader.OpenOrder(o.id, o.tpe, kv._1, o.price, o.amount))).to[mutable.Set]
      }).replay(1).refCount()

      val limitOrderCreatedStream = poloniexApi.accountNotificationStream.filter(_.isInstanceOf[LimitOrderCreated]).map(_.asInstanceOf[LimitOrderCreated])
      val orderUpdateStream = poloniexApi.accountNotificationStream.filter(_.isInstanceOf[OrderUpdate]).map(_.asInstanceOf[OrderUpdate])

      val ordersLimitOrderUpdate = limitOrderCreatedStream.withLatestFrom(initialOrdersStream, (limitOrderCreated, orders: mutable.Set[Trader.OpenOrder]) => {
        logger.info(s"Account info: $limitOrderCreated")

        markets.take(1).map(market => {
          val marketId = market._1.get(limitOrderCreated.marketId)

          if (marketId.isDefined) {
            val newOrder = new Trader.OpenOrder(
              limitOrderCreated.orderNumber,
              limitOrderCreated.orderType,
              marketId.get,
              limitOrderCreated.rate,
              limitOrderCreated.amount,
            )

            orders += newOrder
          } else {
            logger.warn("Market id not found in local cache. Fetching markets from API...")
            trigger.ticker.onNext(())
          }

          orders
        })
      }).concatMap(o => o)

      val ordersUpdate = orderUpdateStream.withLatestFrom(initialOrdersStream, (orderUpdate, orders: mutable.Set[Trader.OpenOrder]) => {
        logger.info(s"Account info: $orderUpdate")

        if (orderUpdate.newAmount == 0) {
          orders.remove(new Trader.OpenOrder(orderUpdate.orderId))
        } else {
          val order = orders.view.find(_.id == orderUpdate.orderId)

          if (order.isDefined) {
            order.get.amount = orderUpdate.newAmount
          } else {
            logger.warn("Order not found in local cache. Fetch orders from the server.")
            trigger.openOrders.onNext(())
          }
        }

        orders
      })

      Flux.merge(initialOrdersStream, ordersLimitOrderUpdate, ordersUpdate).replay(1).refCount()
    }

    val tickers: Flux[mutable.Map[Market, Ticker]] = {
      val allTickersStream = raw.ticker.map(allTickers => mutable.Map(allTickers.toSeq: _*)).replay(1).refCount()

      val tickersUpdate = raw.tickerStream.withLatestFrom(allTickersStream, (ticker, allTickers: mutable.Map[Market, Ticker]) => {
        markets.take(1).map(market => {
          val marketId = market._1.get(ticker.id)

          if (marketId.isDefined) {
            allTickers.put(marketId.get, ticker)
            logger.whenTraceEnabled {
              logger.trace(ticker.toString)
            }
          } else {
            logger.warn("Market not found in local market cache. Updating market cache...")
            trigger.ticker.onNext(())
          }

          allTickers
        })
      }).concatMap(identity)

      Flux.merge(allTickersStream, tickersUpdate).replay(1).refCount()
    }

    val orderBooks: Flux[OrderBookDataMap] = {
      markets.map(marketInfo => {
        val marketIds = marketInfo._1.keys.toArray
        poloniexApi.orderBooksStream(marketIds: _*).map{
          case (marketId, bookStream) =>
            val newBookStream = bookStream.map {
              case (book, update) =>
                (marketInfo._1(marketId), marketId, book, update)
            }
            (marketId, newBookStream)
        }
      }).replay(1).refCount()
    }
  }

  object indicators {
    type MinMaxPrice = (BigDecimal, BigDecimal)
    type SellBuyMinMaxPrice = (MinMaxPrice, MinMaxPrice)

    case class Candlestick(timeStart: LocalDateTime, timeStop: LocalDateTime, low: BigDecimal, close: BigDecimal, open: BigDecimal, high: BigDecimal)

    object Candlestick {
      def create(price: BigDecimal): Candlestick = {
        val t = LocalDateTime.now()
        Candlestick(t, t, price, price, price, price)
      }
    }

    /*val candlestick: Flux[Map[Int, Flux[Candlestick]]] = data.orderBooks.map(_.map {case (marketId, orderBook) =>
      val singleCandlestick = orderBook.map(_._1)
        .map(_.asks.head._1)
        .map(Candlestick.create(_))
        .window(30 seconds)
        .switchMap(_.reduce((candlestick0: Candlestick, candlestick1: Candlestick) => {
          Candlestick(
            candlestick0.timeStart,
            candlestick1.timeStop,
            candlestick0.low.min(candlestick1.low),
            candlestick1.close,
            candlestick0.open,
            candlestick0.high.max(candlestick1.high),
          )
        })
      )

      (marketId, singleCandlestick)
    })

    val closePrices: Flux[Map[Int, Flux[Price]]] = {
      data.orderBooks.map(_.map {case (marketId, orderBook) =>
        val price = orderBook
          .map(_._1)
          .filter(_.asks.nonEmpty)
          .map(_.asks.head._1)
          .sample(1 minute)

        (marketId, price)
      }).share()
    }*/

    val priceMovement: Flux[Trader.Valuation] = Flux.zip(data.markets, data.orderBooks).switchMap {
      case (marketInfo, orderBooks) =>
        val mainTargetCurrencies = ("USDC", "USDT")

        val pathValuations = new MarketPathGenerator(marketInfo._2.keys)
          .generateAll(mainTargetCurrencies)
          .flatMap {
            case (targetCurrencies, simplePaths) =>
              simplePaths.map(simplePath => {
              val pathOrderBooks = simplePath.map(market => orderBooks(marketInfo._2(market.toString)))

              // Main variable that contains calculations
              val valuationFlux = Flux.combineLatest(pathOrderBooks, (orderBooksAnyRefs: Array[AnyRef]) => {
                val orderBooks = orderBooksAnyRefs.view.map(_.asInstanceOf[data.OrderBookData])
                val orderBooks2 = simplePath.zip(orderBooks).map {case (market, (_, _, book, _)) => (market, book)}.toArray
                OrderPlan.generate(targetCurrencies, orderBooks2)
              })

              valuationFlux.replay(1).refCount()
            })
          }.toSeq // TODO: Review to seq

        Flux.just(Flux.empty[Trader.Valuation], pathValuations: _*)
          .skip(1)
          .flatMap(identity, pathValuations.length)
    }.share()
  }

  /*object sellBuyIndicators {
    trait BuyTrait
    trait SellTrait

    case class SimpleBuy(prices: List[Price]) extends BuyTrait
    case class SimpleSell(prices: List[Price]) extends SellTrait

    object buy {
      val simple: Flux[Map[Int, Flux[SimpleBuy]]] = indicators.closePrices.map(marketPrice => {
        marketPrice.map(mp => {
          val x = mp._2.buffer(3).filter(buffer => {
            (1 until buffer.length).forall(i => buffer(i-1) < buffer(i))
          }).map(buffer => SimpleBuy(buffer.toList))

          (mp._1, x)
        })
      })
    }

    object sell {
      val simple: Flux[Map[Int, Flux[SimpleSell]]] = indicators.closePrices.map(marketPrice => {
        marketPrice.map(mp => {
          val x = mp._2.buffer(3).filter(buffer => {
            (1 until buffer.length).forall(i => buffer(i-1) > buffer(i))
          }).map(buffer => SimpleSell(buffer.toList))

          (mp._1, x)
        })
      })
    }

    val buyStream: Flux[(Market, BuyTrait)] = buy.simple.map(_.map(t => t._2.map(a => (t._1, a))))
      .flatMapIterable(identity)
      .flatMap(identity)
      .concatMap(marketAdvice =>
        data.markets.take(1).map(market => (market._1(marketAdvice._1), marketAdvice._2))
      )

    val sellStream: Flux[(Market, SellTrait)] = sell.simple.map(_.map(t => t._2.map(a => (t._1, a))))
      .flatMapIterable(identity)
      .flatMap(identity)
      .concatMap(marketAdvice =>
        data.markets.take(1).map(market => (market._1(marketAdvice._1), marketAdvice._2))
      )
  }*/

  def start(): Flux[Unit] = {
    Flux.create((sink: FluxSink[Unit]) => {
      val defaultErrorHandler = (error: Throwable) => {
        error.printStackTrace()
      }

      val defaultCompleted = () => {
        logger.debug("completed")
      }

      //val currencies = data.currencies.subscribe(currencies => {}, defaultErrorHandler, defaultCompleted)
      //val balances = data.balances.subscribe(balances => {}, defaultErrorHandler, defaultCompleted)
      val markets = data.markets.subscribe(markets => {}, defaultErrorHandler, defaultCompleted)
      //val openOrders = data.openOrders.subscribe(orders => {}, defaultErrorHandler, defaultCompleted)
      //val tickers = data.tickers.subscribe(tickers => {}, defaultErrorHandler, defaultCompleted)


      data.tickers.subscribe(_ => {}, _.printStackTrace())

      val x = data.orderBooks.map(_.values).switchMap(Flux.merge(_))

      x.subscribe((market: (Market, MarketId, PriceAggregatedBook, OrderBookNotification)) => {
        //logger.debug(s"book update from market ${market._1}")

      })

      data.tradesStat.switchMap {orderBookStat =>
        orderBookStat(121)
      }.subscribe(stat => {
        import io.circe.generic.auto._
        import io.circe.syntax._
        logger.info(stat.asJson.spaces2)
      })

      /*indicators.priceMovement.subscribe(valuation => {
        logger.debug(valuation.toString)
      }, defaultErrorHandler, defaultCompleted)

      // TODO: Test trigger. Fails with exception
      Flux.just(1).delaySubscription(30 seconds).subscribe(_ => {
        trigger.ticker.onNext(())
      })

      val disposable = new Disposable {
        override def dispose(): Unit = {
          markets.dispose()
          currencies.dispose()
          balances.dispose()
          openOrders.dispose()
          //tickers.dispose()
          //orderBooks.dispose()
        }
      }

      sink.onCancel(disposable)
      sink.onDispose(disposable)*/

      logger.info("Start trading")
    })
  }
}

object Trader {
  class OpenOrder(
    val id: Long,
    val tpe: OrderType,
    val market: Market,
    val rate: BigDecimal,
    var amount: BigDecimal,
  ) {

    def this(id: Long) {
      this(id, OrderType.Sell, "BTC_ETH", 0, 0)
    }

    override def hashCode(): Int = id.hashCode()

    override def equals(o: Any): Boolean = o match {
      case order: OpenOrder => order.id == this.id
      case _ => false
    }

    override def toString = s"OpenOrder($id, $tpe, $market, $rate, $amount)"
  }

  case class Valuation(shortPath: ShortPath, longPath: List[Market], k: BigDecimal)

  case class ShortPath(fromCurrency: Currency, toCurrency: Currency) {
    override def toString: Currency = s"$fromCurrency->$toCurrency"
  }

  object OrderPlan {
    import MarketPathGenerator._

    def generate(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      path.length match {
        case 1 => f1(targetPath, path)
        case 2 => f2(targetPath, path)
        case 3 => f3(targetPath, path)
        case 4 => f4(targetPath, path)
        case _ => throw new NotImplementedError("Operation not supported")
      }
    }

    private def f1(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)

      // TODO: Handle a -> b and b -> a

      // a_b
      val a = m1.find(Iterable(targetPath._1, targetPath._2)).get
      val b = m1.other(a).get

      val k = op(b, m1, b1) // a_b

      Valuation(ShortPath(a, b), List(m1), k)
    }

    private def f2(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)
      val (m2, b2) = path(1)

      // a_c - b_c
      val a = m1.find(Iterable(targetPath._1, targetPath._2)).get
      val c = m1.other(a).get
      val b = m2.other(c).get

      val k = op(c, m1, b1)*op(b, m2, b2)

      Valuation(ShortPath(a, b), List(m1, m2), k)
    }

    private def f3(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)
      val (m2, b2) = path(1)
      val (m3, b3) = path(2)

      // a_x - x_y - b_y
      val a = m1.find(Iterable(targetPath._1, targetPath._2)).get
      val x = m1.other(a).get
      val y = m2.other(x).get
      val b = m3.other(y).get

      val k = op(x, m1, b1)*op(y, m2, b2)*op(b, m3, b3)

      Valuation(ShortPath(a, b), List(m1, m2, m3), k)
    }

    private def f4(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)
      val (m2, b2) = path(1)
      val (m3, b3) = path(2)
      val (m4, b4) = path(3)

      // a_x - x_z - z_y - b_y
      val a = m1.find(Iterable(targetPath._1, targetPath._2)).get
      val x = m1.other(a).get
      val z = m2.other(x).get
      val y = m3.other(z).get
      val b = m4.other(y).get

      val k = op(x, m1, b1)*op(z, m2, b2)*op(y, m3, b3)*op(b, m4, b4)

      Valuation(ShortPath(a, b), List(m1, m2, m3, m4), k)
    }

    private def op(targetCurrency: Currency, market: Market, orderBook: PriceAggregatedBook): Price = {
      market.tpe(targetCurrency).get match {
        case CurrencyType.Base => sell(orderBook)
        case CurrencyType.Quote => buy(orderBook)
      }
    }

    private def buy(orderBook: PriceAggregatedBook): Price = {
      BigDecimal(1) / orderBook.bids.head._1
    }

    private def sell(orderBook: PriceAggregatedBook): Price = {
      orderBook.asks.head._1
    }
  }

  class MarketPathGenerator(availableMarkets: Iterable[Market]) {
    import MarketPathGenerator._

    private val allPaths = availableMarkets
      .view
      .flatMap(m => Set(((m.b, m.q), m), ((m.q, m.b), m)))
      .groupBy(p => p._1._1)
      .map(x => (x._1, x._2.toSet))

    def generate(targetPath: TargetPath): Set[Path] = {
      f(targetPath).filter(_.nonEmpty)
    }

    def generateAll(targetPath: TargetPath): Map[TargetPath, Set[Path]] = {
      val (a,b) = targetPath
      val p = Array((a,a), (a,b), (b,a), (b,b))

      Map(
        p(0) -> generate(p(0)),
        p(1) -> generate(p(1)),
        p(2) -> generate(p(2)),
        p(3) -> generate(p(3)),
      )
    }

    private def f1(targetPath: TargetPath): Set[Path] = {
      allPaths(targetPath._1)
        .find(_._1._2 == targetPath._2)
        .map(m => Set(List(m._2)))
        .getOrElse({Set(List())})
    }

    private def f2(targetPath: TargetPath): Iterable[Path] = {
      // (a->x - y->b)
      val (p,q) = targetPath
      for {
        ((_,x),m1) <- allPaths(p).view.filter(_._1._2 != q)
        (_,m2) <- allPaths(x).view.filter(_._1._2 == q)
      } yield List(m1,m2)
    }

    private def f3(targetPath: TargetPath): Iterable[Path] = {
      // (a->x - y->z - k->b)
      val (p,q) = targetPath
      for {
        ((_,x),m1) <- allPaths(p).view.filter(_._1._2 != q)
        ((_,z),m2) <- allPaths(x).view.filter(h => h._1._2 != p && h._1._2 != q)
        (_,m3) <- allPaths(z).view.filter(_._1._2 == q)
      } yield List(m1,m2,m3)
    }

    private def f4(targetPath: TargetPath): Iterable[Path] = {
      // (a->x - y->z - i->j - k->b)
      val (p,q) = targetPath
      for {
        ((_,x),m1) <- allPaths(p).view.filter(_._1._2 != q)
        ((y,z),m2) <- allPaths(x).view.filter(h => h._1._2 != p && h._1._2 != q)
        ((_,j),m3) <- allPaths(z).view.filter(h => h._1._2 != p && h._1._2 != q && h._1._2 != y)
        (_,m4) <- allPaths(j).view.filter(_._1._2 == q)
      } yield List(m1,m2,m3,m4)
    }

    private def f(targetPath: TargetPath): Set[Path] = {
      f1(targetPath) ++ f2(targetPath) ++ f3(targetPath) ++ f4(targetPath)
    }
  }
  
  object MarketPathGenerator {
    type TargetPath = (Currency, Currency)
    type Path = List[Market]
  }


  object TradeStatModels {
    case class SimpleTrade(
      price: Price,
      amount: Amount,
      timestamp: Instant,
    )

    case class Trade0(
      sell: Vector[SimpleTrade],
      buy: Vector[SimpleTrade],
    )

    case class Trade1(
      sellOld: Vector[SimpleTrade],
      sellNew: Vector[SimpleTrade],
      buyOld: Vector[SimpleTrade],
      buyNew: Vector[SimpleTrade],
    )

    object Trade1 {
      @inline def newTrades(newTrade: SimpleTrade, trades: Vector[SimpleTrade], limit: Int): Vector[SimpleTrade] = {
        if (trades.length == limit) {
          newTrade +: trades.dropRight(1)
        } else {
          newTrade +: trades
        }
      }
    }

    case class Trade2(
      sell: Trade2State,
      buy: Trade2State,
    )

    case class Trade2State(
      ttwSumMs: BigDecimal,
      ttwVarianceSum: BigDecimal,
      sumAmount: BigDecimal,
      varianceSumAmount: BigDecimal,

      ttwAverageMs: BigDecimal,
      ttwVariance: BigDecimal,
      ttwStdDev: BigDecimal,
      minAmount: BigDecimal,
      maxAmount: BigDecimal,
      avgAmount: BigDecimal,
      varianceAmount: BigDecimal,
      stdDevAmount: BigDecimal,
      firstTranTs: Instant,
      lastTranTs: Instant,
    )

    object Trade2State {
      private final val tsNull: Long = -1

      @inline private def subSquare(a: BigDecimal, b: BigDecimal): BigDecimal = {
        val x = a - b
        x * x
      }

      def map(stat: Trade2State): TradeStatOrder = {
        TradeStatOrder(
          stat.ttwAverageMs.toLong,
          stat.ttwVariance.toLong,
          stat.ttwStdDev.toLong,
          stat.minAmount,
          stat.maxAmount,
          stat.avgAmount,
          stat.varianceAmount,
          stat.stdDevAmount,
          stat.firstTranTs,
          stat.lastTranTs,
        )
      }
      
      def from0() : Trade2State = {
        Trade2State(
          ttwSumMs = 0,
          ttwVarianceSum = 0,
          sumAmount = 0,
          varianceSumAmount = 0,

          ttwAverageMs = Double.MaxValue,
          ttwVariance = Double.MaxValue,
          ttwStdDev = Double.MaxValue,
          minAmount = Double.MaxValue,
          maxAmount = Double.MinValue,
          avgAmount = 0,
          varianceAmount = Double.MaxValue,
          stdDevAmount = Double.MaxValue,
          firstTranTs = Instant.EPOCH,
          lastTranTs = Instant.EPOCH,
        )
      }

      def from1(trade: SimpleTrade) : Trade2State = {
        Trade2State(
          ttwSumMs = 0,
          ttwVarianceSum = 0,
          sumAmount = trade.amount,
          varianceSumAmount = 0,

          ttwAverageMs = trade.timestamp.toEpochMilli,
          ttwVariance = 0,
          ttwStdDev = 0,
          minAmount = trade.amount,
          maxAmount = trade.amount,
          avgAmount = trade.amount,
          varianceAmount = 0,
          stdDevAmount = 0,
          firstTranTs = trade.timestamp,
          lastTranTs = trade.timestamp,
        )
      }

      def calc(state: Trade2State, old: Vector[SimpleTrade], `new`: Vector[SimpleTrade]): Trade2State = {
        if (`new`.length == 1) {
          from1(`new`.head)
        } else {
          val newTrade = `new`.head
          val oldTrade = old.last

          var ttwSumMs: BigDecimal = 0
          var ttwVarianceSum: BigDecimal = 0
          var sumAmount: BigDecimal = 0
          var varianceSumAmount: BigDecimal = 0

          var ttwAverageMs: BigDecimal = 0
          var ttwVariance: BigDecimal = 0
          var ttwStdDev: BigDecimal = 0
          val minAmount = state.minAmount min newTrade.amount
          val maxAmount = state.maxAmount max newTrade.amount
          var avgAmount: BigDecimal = 0
          var varianceAmount: BigDecimal = 0
          var stdDevAmount: BigDecimal = 0
          val firstTranTs: Instant = `new`.last.timestamp
          val lastTranTs: Instant = newTrade.timestamp

          if (`new`.length == old.length) {
            sumAmount = state.sumAmount + newTrade.amount - oldTrade.amount
            varianceSumAmount = {
              val oldAvgAmount = state.sumAmount / old.length
              val deltaAmountAvg = (newTrade.amount - oldTrade.amount) / old.length
              val deltaVarianceSumAmount = {
                val newAmountValue = subSquare(newTrade.amount, oldAvgAmount)
                val oldAmountValue = subSquare(oldTrade.amount, oldAvgAmount)
                newAmountValue - oldAmountValue + deltaAmountAvg * (old.length * (deltaAmountAvg + 2 * oldAvgAmount) - 2 * sumAmount)
              }
              state.varianceSumAmount + deltaVarianceSumAmount
            }

            val newTtwValue = newTrade.timestamp.toEpochMilli - `new`(1).timestamp.toEpochMilli
            val oldTtwValue = old(old.length - 2).timestamp.toEpochMilli - oldTrade.timestamp.toEpochMilli

            ttwSumMs = state.ttwSumMs + newTtwValue - oldTtwValue
            ttwVarianceSum = {
              val size = old.length - 1
              val oldAvgTtw = state.ttwSumMs / size
              val deltaAvgTtw = (newTtwValue - oldTtwValue) / size.toDouble
              val deltaVarianceSumTtw = {
                val newTtwValue0 = subSquare(newTtwValue, oldAvgTtw)
                val oldTtwValue0 = subSquare(oldTtwValue, oldAvgTtw)
                newTtwValue0 - oldTtwValue0 + deltaAvgTtw * (size * (deltaAvgTtw + 2 * oldAvgTtw) - 2 * ttwSumMs)
              }
              state.ttwVarianceSum + deltaVarianceSumTtw
            }
          } else {
            sumAmount = state.sumAmount + newTrade.amount
            varianceSumAmount = {
              val oldAvgAmount = state.sumAmount / old.length
              val deltaAmountAvg = (old.length * newTrade.amount - state.sumAmount) / (old.length * `new`.length)
              val deltaVarianceSumAmount = {
                val newAmountValue = subSquare(newTrade.amount, oldAvgAmount)
                newAmountValue + deltaAmountAvg * (`new`.length * (deltaAmountAvg + 2 * oldAvgAmount) - 2 * sumAmount)
              }
              state.varianceSumAmount + deltaVarianceSumAmount
            }

            val newTtwValue = newTrade.timestamp.toEpochMilli - `new`(1).timestamp.toEpochMilli
            ttwSumMs = state.ttwSumMs + newTtwValue
            ttwVarianceSum = {
              if (old.length == 1) {
                0
              } else {
                val oldSize = old.length - 1
                val newSize = old.length
                val oldAvgTtw = state.ttwSumMs / oldSize
                val deltaAvgTtw = (oldSize * newTtwValue - state.ttwSumMs) / (oldSize * newSize)
                val deltaVarianceSumTtw = {
                  val newTtwValue0 = subSquare(newTtwValue, oldAvgTtw)
                  newTtwValue0 + deltaAvgTtw * (newSize * (deltaAvgTtw + 2 * oldAvgTtw) - 2 * ttwSumMs)
                }
                state.ttwVarianceSum + deltaVarianceSumTtw
              }
            }
          }

          val size = `new`.length
          ttwAverageMs = ttwSumMs / (size - 1)
          ttwVariance = ttwVarianceSum / (size - 1)
          ttwStdDev = Math.sqrt(ttwVariance.toDouble)
          avgAmount = sumAmount / size
          varianceAmount = varianceSumAmount / size
          stdDevAmount = Math.sqrt(varianceAmount.toDouble)

          Trade2State(
            ttwSumMs,
            ttwVarianceSum,
            sumAmount,
            varianceSumAmount,

            ttwAverageMs,
            ttwVariance,
            ttwStdDev,
            minAmount,
            maxAmount,
            avgAmount,
            varianceAmount,
            stdDevAmount,
            firstTranTs,
            lastTranTs,
          )
        }
      }

      def calcFull(trades: Vector[SimpleTrade]): Trade2State = {
        if (trades.isEmpty) {
          from0()
        } else if (trades.length == 1) {
          from1(trades.head)
        } else {
          val tradesSize = trades.length

          var ttwSumMs: BigDecimal = 0
          var ttwVarianceSum: BigDecimal = 0
          var sumAmount: BigDecimal = 0
          var varianceSumAmount: BigDecimal = 0

          var ttwAverageMs: BigDecimal = 0
          var ttwVariance: BigDecimal = 0
          var ttwStdDev: BigDecimal = 0
          var minAmount: BigDecimal = Double.MaxValue
          var maxAmount: BigDecimal = Double.MinValue
          var avgAmount: BigDecimal = 0
          var varianceAmount: BigDecimal = 0
          var stdDevAmount: BigDecimal = 0
          val firstTranTs: Instant = trades.last.timestamp
          val lastTranTs: Instant = trades.head.timestamp

          var a: Long = tsNull
          for (t <- trades) {
            if (t.amount < minAmount) minAmount = t.amount
            if (t.amount > maxAmount) maxAmount = t.amount
            sumAmount += t.amount

            if (a == tsNull) {
              a = t.timestamp.toEpochMilli
            } else {
              val b = t.timestamp.toEpochMilli
              ttwSumMs += a - b
              a = b
            }
          }

          a = tsNull

          ttwAverageMs = ttwSumMs / (tradesSize - 1)
          avgAmount = sumAmount / tradesSize

          for (t <- trades) {
            varianceSumAmount += subSquare(t.amount, avgAmount)

            if (a == tsNull) {
              a = t.timestamp.toEpochMilli
            } else {
              val b = t.timestamp.toEpochMilli
              ttwVarianceSum += subSquare(a - b, ttwAverageMs)
              a = b
            }
          }

          ttwVariance = ttwVarianceSum / (tradesSize - 1)
          ttwStdDev = Math.sqrt(ttwVariance.toDouble)
          varianceAmount = varianceSumAmount / tradesSize
          stdDevAmount = Math.sqrt(varianceAmount.toDouble)

          Trade2State(
            ttwSumMs,
            ttwVarianceSum,
            sumAmount,
            varianceSumAmount,

            ttwAverageMs,
            ttwVariance,
            ttwStdDev,
            minAmount,
            maxAmount,
            avgAmount,
            varianceAmount,
            stdDevAmount,
            firstTranTs,
            lastTranTs,
          )
        }
      }
    }
  }
}