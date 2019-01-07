package com.gitlab.dhorman.cryptotrader.trader

import java.time.LocalDateTime

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi._
import com.gitlab.dhorman.cryptotrader.trader.Trader.{MarketPathGenerator, OrderPlan}
import com.typesafe.scalalogging.Logger
import reactor.core.Disposable
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
      triggerStream.startWith(()).flatMap(_ => stream.take(1)).replay(1).refCount()
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
      }).replay(1).refCount()
    }

    val openOrders: Flux[mutable.Set[Trader.OpenOrder]] = {
      val initialOrdersStream: Flux[mutable.Set[Trader.OpenOrder]] = raw.openOrders.map(orders => {
        orders.flatMap(kv => kv._2.view.map(o => new Trader.OpenOrder(o.id, o.tpe, kv._1, o.rate, o.amount))).to[mutable.Set]
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


      val x = data.orderBooks.map(_.values).switchMap(Flux.merge(_))

      x.subscribe((market: (Market, MarketId, PriceAggregatedBook, OrderBookNotification)) => {
        //logger.debug(s"book update from market ${market._1}")

      })

      /*indicators.priceMovement.subscribe(valuation => {
        logger.debug(valuation.toString)
      }, defaultErrorHandler, defaultCompleted)*/

      // TODO: Test trigger. Fails with exception
      /*Flux.just(1).delaySubscription(30 seconds).subscribe(_ => {
        trigger.ticker.onNext(())
      })*/

      val disposable = new Disposable {
        override def dispose(): Unit = {
          markets.dispose()
          /*currencies.dispose()
          balances.dispose()
          openOrders.dispose()*/
          //tickers.dispose()
          //orderBooks.dispose()
        }
      }

      sink.onCancel(disposable)
      sink.onDispose(disposable)

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

  case class Valuation(shortPath: ShortPath, longPath: List[MarketPathGenerator.Market], k: BigDecimal)

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

    private def f1(targetCurrencies: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)

      // TODO: Handle a -> b and b -> a

      // a_b
      val a = m1.main(targetCurrencies)
      val b = m1.other(a)

      val k = op(b, m1, b1) // a_b

      Valuation(ShortPath(a, b), List(m1), k)
    }

    private def f2(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)
      val (m2, b2) = path(1)

      // a_c - b_c
      val a = m1.main(targetPath)
      val c = m1.other(a)
      val b = m2.other(c)

      val k = op(c, m1, b1)*op(b, m2, b2)

      Valuation(ShortPath(a, b), List(m1, m2), k)
    }

    private def f3(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)
      val (m2, b2) = path(1)
      val (m3, b3) = path(2)

      // a_x - x_y - b_y
      val a = m1.main(targetPath)
      val x = m1.other(a)
      val y = m2.other(x)
      val b = m3.other(y)

      val k = op(x, m1, b1)*op(y, m2, b2)*op(b, m3, b3)

      Valuation(ShortPath(a, b), List(m1, m2, m3), k)
    }

    private def f4(targetPath: TargetPath, path: Array[(Market, PriceAggregatedBook)]): Valuation = {
      val (m1, b1) = path(0)
      val (m2, b2) = path(1)
      val (m3, b3) = path(2)
      val (m4, b4) = path(3)

      // a_x - x_z - z_y - b_y
      val a = m1.main(targetPath)
      val x = m1.other(a)
      val z = m2.other(x)
      val y = m3.other(z)
      val b = m4.other(y)

      val k = op(x, m1, b1)*op(z, m2, b2)*op(y, m3, b3)*op(b, m4, b4)

      Valuation(ShortPath(a, b), List(m1, m2, m3, m4), k)
    }

    private def op(targetCurrency: Currency, market: Market, orderBook: PriceAggregatedBook): Price = {
      market.tpe(targetCurrency) match {
        case Market.CurrencyType.Master => sell(orderBook)
        case Market.CurrencyType.Slave => buy(orderBook)
      }
    }

    private def buy(orderBook: PriceAggregatedBook): Price = {
      BigDecimal(1) / orderBook.bids.head._1
    }

    private def sell(orderBook: PriceAggregatedBook): Price = {
      orderBook.asks.head._1
    }
  }

  class MarketPathGenerator(availableMarkets: Iterable[MarketPathGenerator.Market]) {
    import MarketPathGenerator._

    private val allPaths: Map[TargetPath, Market] = availableMarkets
      .view
      .flatMap(m => Set(((m.a, m.b), m), ((m.b, m.a), m)))
      .toMap

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
      allPaths.get(targetPath)
        .map(m => Set(List(m)))
        .getOrElse({Set(List())})
    }

    private def f2(targetPath: TargetPath): Iterable[Path] = {
      // (a->x - y->b)
      val (p,q) = targetPath
      for {
        ((_,x),m1) <- allPaths.view.filter(h => h._1._1 == p && h._1._2 != q)
        (_,m2) <- allPaths.view.filter(h => h._1._1 == x && h._1._2 == q)
      } yield List(m1,m2)
    }

    private def f3(targetPath: TargetPath): Iterable[Path] = {
      // (a->x - y->z - k->b)
      val (p,q) = targetPath
      for {
        ((_,x),m1) <- allPaths.view.filter(h => h._1._1 == p && h._1._2 != q)
        ((_,z),m2) <- allPaths.view.filter(h => h._1._1 == x && h._1._2 != p && h._1._2 != q)
        (_,m3) <- allPaths.view.filter(h => h._1._1 == z && h._1._2 == q)
      } yield List(m1,m2,m3)
    }

    private def f4(targetPath: TargetPath): Iterable[Path] = {
      // (a->x - y->z - i->j - k->b)
      val (p,q) = targetPath
      for {
        ((_,x),m1) <- allPaths.view.filter(h => h._1._1 == p && h._1._2 != q)
        ((y,z),m2) <- allPaths.view.filter(h => h._1._1 == x && h._1._2 != p && h._1._2 != q)
        ((_,j),m3) <- allPaths.view.filter(h => h._1._1 == z && h._1._2 != p && h._1._2 != q && h._1._2 != y)
        (_,m4) <- allPaths.view.filter(h => h._1._1 == j && h._1._2 == q)
      } yield List(m1,m2,m3,m4)
    }

    private def f(targetPath: TargetPath): Set[Path] = {
      f1(targetPath) ++ f2(targetPath) ++ f3(targetPath) ++ f4(targetPath)
    }
  }
  
  object MarketPathGenerator {
    case class Market(a: Currency, b: Currency) {
      override def toString: String = s"${a}_$b"

      override def equals(o: Any): Boolean = o match {
        case m: Market => (m.a == this.a || m.a == this.b) && (m.b == this.a || m.b == this.b)
        case _ => false
      }

      override def hashCode(): Int = a.hashCode + b.hashCode

      def masterCurrency: Currency = a
      def slaveCurrency: Currency = b

      def tpe(x: Currency): Market.CurrencyType = {
        if (x == a) return Market.CurrencyType.Master
        if (x == b) return Market.CurrencyType.Slave
        throw new IllegalStateException("Must be passed valid value")
      }

      def main(targetCurrencies: TargetPath): Currency = {
        val c = targetCurrencies._1
        val d = targetCurrencies._2

        if (a == c || a == d) return a
        if (b == c || b == d) return b

        throw new IllegalStateException()
      }

      def contains(x: Currency): Boolean = x == a || x == b

      def other(x: Currency): Currency = if (x == a) b else a
    }

    object Market {
      type CurrencyType = CurrencyType.Value
      object CurrencyType extends Enumeration {
        val Master: CurrencyType = Value
        val Slave: CurrencyType = Value
      }
    }

    type TargetPath = (Currency, Currency)
    type Path = List[Market]

    implicit def convert(markets: Iterable[String]): Iterable[Market] = {
      markets.map(_.split('_')).filter(_.length == 2).map(c => Market(c(0), c(1)))
    }

    implicit def convert(market: String): Market = {
      val c = market.split('_')
      require(c.length == 2, "market must be in format A_B. Example: USDT_USDC.")
      Market(c(0), c(1))
    }
  }
}