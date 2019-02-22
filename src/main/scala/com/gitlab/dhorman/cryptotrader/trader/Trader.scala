package com.gitlab.dhorman.cryptotrader.trader

import java.time.Instant

import com.gitlab.dhorman.cryptotrader.core.MarketPathGenerator.{ExhaustivePath, PathOrderType, TargetPath}
import com.gitlab.dhorman.cryptotrader.core.Orders.InstantDelayedOrder
import com.gitlab.dhorman.cryptotrader.core.Prices._
import com.gitlab.dhorman.cryptotrader.core._
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi._
import com.gitlab.dhorman.cryptotrader.util.ReactorUtil._
import com.typesafe.scalalogging.Logger
import reactor.core.scala.publisher.{Flux, FluxProcessor}

import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer

class Trader(private val poloniexApi: PoloniexApi) {
  private val logger = Logger[Trader]

  private object trigger {
    type Trigger = FluxProcessor[Unit, Unit]

    val currencies: Trigger = createReplayProcessor()
    val ticker: Trigger = createReplayProcessor()
    val balances: Trigger = createReplayProcessor()
    val openOrders: Trigger = createReplayProcessor()
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
    type MarketIntMap = Map[MarketId, Market]
    type MarketStringMap = Map[Market, MarketId]
    type MarketData = (MarketIntMap, MarketStringMap)
    type OrderBookData = (Market, MarketId, PriceAggregatedBook, OrderBookNotification)
    type OrderBookDataMap = Map[MarketId, Flux[OrderBookData]]

    val currencies: Flux[(Map[Currency, CurrencyDetails], Map[Int, Currency])] = {
      raw.currencies.map(curr => {
        (curr, curr.map(kv => (kv._2.id, kv._1)))
      }).cache(1)
    }

    val balances: Flux[Map[Currency, Amount]] = {
      raw.balances.switchMap { balanceInfo =>
        currencies.switchMap { curr =>
          raw.accountNotifications
            .filter(_.isInstanceOf[BalanceUpdate])
            .map(_.asInstanceOf[BalanceUpdate])
            .scan(balanceInfo, (allBalances: Map[Currency, Amount], balanceUpdate: BalanceUpdate) => {
              if (balanceUpdate.walletType == WalletType.Exchange) {
                val currAmountOption = curr._2
                  .get(balanceUpdate.currencyId)
                  .flatMap { currencyId =>
                    allBalances.get(currencyId).map(balance => (currencyId, balance))
                  }

                if (currAmountOption.isDefined) {
                  val (currencyId, balance) = currAmountOption.get
                  val newBalance = balance + balanceUpdate.amount

                  allBalances.updated(currencyId, newBalance)
                } else {
                  logger.warn("Balances and currencies can be not in sync. Fetch new balances and currencies.")
                  trigger.currencies.onNext(())
                  trigger.balances.onNext(())

                  allBalances
                }
              } else {
                allBalances
              }
            }
          )
        }
      }.cache(1)
    }

    val markets: Flux[MarketData] = {
      raw.ticker.map { tickers =>
        var marketIntStringMap = Map[MarketId, Market]()
        var marketStringIntMap = Map[Market, MarketId]()

        // TODO: Review frozen filter
        tickers.view.filter(!_._2.isFrozen)
        .foreach(tick => {
          marketIntStringMap = marketIntStringMap.updated(tick._2.id, tick._1)
          marketStringIntMap = marketStringIntMap.updated(tick._1, tick._2.id)
        })

        (marketIntStringMap, marketStringIntMap)
      }.doOnNext { markets =>
        logger.info("Markets fetched")
        logger.whenDebugEnabled {
          logger.debug(markets.toString)
        }
      }.cache(1)
    }

    val tradesStat: Flux[Map[MarketId, Flux[TradeStat]]] = {
      import Trader.TradeStatModels._

      val BufferLimit = 100

      markets.map {case (_, marketInfo) =>
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
      }.cache(1)
    }

    val openOrders: Flux[Map[Long, Trader.OpenOrder]] = {
      val initialOrdersStream: Flux[Map[Long, Trader.OpenOrder]] = raw.openOrders.map { marketOrdersMap =>
        marketOrdersMap.flatMap {case (market, ordersSet) =>
          ordersSet.map(o => new Trader.OpenOrder(o.id, o.tpe, market, o.price, o.amount))
        }.map {order =>
          (order.id, order)
        }.toMap
      }

      val limitOrderCreatedStream = poloniexApi.accountNotificationStream.filter(_.isInstanceOf[LimitOrderCreated]).map(_.asInstanceOf[LimitOrderCreated])
      val orderUpdateStream = poloniexApi.accountNotificationStream.filter(_.isInstanceOf[OrderUpdate]).map(_.asInstanceOf[OrderUpdate])

      val orderUpdates = Flux.merge[AccountNotification](limitOrderCreatedStream, orderUpdateStream)

      Flux.combineLatest[Map[Long, Trader.OpenOrder], MarketData, Flux[Map[Long, Trader.OpenOrder]]](initialOrdersStream, markets, (initOrders, marketsInfo) => {
        orderUpdates.scan(initOrders, (orders: Map[Long, Trader.OpenOrder], update: AccountNotification) => {
          update match {
            case limitOrderCreated: LimitOrderCreated =>
              logger.info(s"Account info: $limitOrderCreated")

              val marketId = marketsInfo._1.get(limitOrderCreated.marketId)

              if (marketId.isDefined) {
                val newOrder = new Trader.OpenOrder(
                  limitOrderCreated.orderNumber,
                  limitOrderCreated.orderType,
                  marketId.get,
                  limitOrderCreated.rate,
                  limitOrderCreated.amount,
                )

                orders.updated(newOrder.id, newOrder)
              } else {
                logger.warn("Market id not found in local cache. Fetching markets from API...")
                trigger.ticker.onNext(())
                orders
              }
            case orderUpdate: OrderUpdate =>
              logger.info(s"Account info: $orderUpdate")

              if (orderUpdate.newAmount == 0) {
                orders - orderUpdate.orderId
              } else {
                val order = orders.get(orderUpdate.orderId)

                if (order.isDefined) {
                  val oldOrder = order.get
                  val newOrder = new Trader.OpenOrder(
                    oldOrder.id,
                    oldOrder.tpe,
                    oldOrder.market,
                    oldOrder.rate,
                    orderUpdate.newAmount,
                  )

                  orders.updated(oldOrder.id, newOrder)
                } else {
                  logger.warn("Order not found in local cache. Fetch orders from the server.")
                  trigger.openOrders.onNext(())
                  orders
                }
              }
            case other =>
              logger.warn(s"Received not recognized order update event: $other")
              orders
          }
        })
      }).concatMap(identity).cache(1)
    }

    val tickers: Flux[Map[Market, Ticker]] = {
      markets.switchMap { marketInfo =>
        raw.ticker.switchMap { allTickers =>
          raw.tickerStream.scan(allTickers, (tickers: Map[Market, Ticker], ticker: Ticker) => {
            val marketId = marketInfo._1.get(ticker.id)

            if (marketId.isDefined) {
              logger.whenTraceEnabled {
                logger.trace(ticker.toString)
              }
              tickers.updated(marketId.get, ticker)
            } else {
              logger.warn("Market not found in local market cache. Updating market cache...")
              trigger.ticker.onNext(())
              allTickers
            }
          })
        }
      }.cache(1)
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
      }).cache(1)
    }

    val fee: Flux[FeeMultiplier] = {
      poloniexApi.feeInfo()
        .map{fee =>
          FeeMultiplier(fee.makerFee.oneMinus, fee.takerFee.oneMinus)
        }.flux().cache(1)
    }
  }

  object indicators {
    val paths: Flux[TreeSet[ExhaustivePath]] = Flux.combineLatest(data.markets, data.orderBooks, data.tradesStat, data.fee, support.pathsSettings, (data0: Array[AnyRef]) => {
      val (_, marketInfoStringMap) = data0(0).asInstanceOf[data.MarketData]
      val orderBooks = data0(1).asInstanceOf[data.OrderBookDataMap]
      val stats = data0(2).asInstanceOf[Map[MarketId, Flux[TradeStat]]]
      val fee = data0(3).asInstanceOf[FeeMultiplier]
      val settings = data0(4).asInstanceOf[support.PathsSettings]

      val pathsPermutations = support.PathsUtil.generateSimplePaths(marketInfoStringMap.keys, settings.currencies)
      val pathsPermutationsDelta = support.PathsUtil.wrapPathsPermutationsToStream(pathsPermutations, orderBooks, stats, marketInfoStringMap, settings.initialAmount, fee)

      pathsPermutationsDelta.scan(TreeSet[ExhaustivePath]()(support.ExhaustivePathOrdering), (state: TreeSet[ExhaustivePath], delta: ExhaustivePath) => state + delta)
    }).switchMap(identity).share()

    object support {
      case class PathsSettings(
        initialAmount: Amount,
        currencies: List[Currency],
      )

      val pathsSettings: FluxProcessor[PathsSettings, PathsSettings] = {
        val defaultSettings = PathsSettings(40, List("USDT", "USDC"))
        createReplayProcessor(Some(defaultSettings))
      }

      object PathsUtil {
        def generateSimplePaths(markets: Iterable[Market], currencies: Iterable[Currency]): Map[(Currency, Currency), Set[List[(PathOrderType, Market)]]] = {
          new MarketPathGenerator(markets)
            .generateAllPermutationsWithOrders(currencies)
        }

        def wrapPathsPermutationsToStream(pathsPermutations: Map[(Currency, Currency), Set[List[(PathOrderType, Market)]]], orderBooks: data.OrderBookDataMap, stats: Map[MarketId, Flux[TradeStat]], marketInfoStringMap: data.MarketStringMap, initialAmount: Amount, fee: FeeMultiplier): Flux[ExhaustivePath] = {

          val pathsIterable: Iterable[Flux[ExhaustivePath]] = for {
            (targetPath, paths) <- pathsPermutations
            path <- paths
          } yield {
            val dependencies = new ListBuffer[Flux[AnyRef]]()

            for {
              (tpe, market) <- path
            } {
              val marketId = marketInfoStringMap(market)
              val orderBook = orderBooks(marketId).map(_._3.asInstanceOf[AnyRef])
              dependencies += orderBook

              tpe match {
                case PathOrderType.Delayed =>
                  val stat = stats(marketId).map(_.asInstanceOf[AnyRef])
                  dependencies += stat
                case PathOrderType.Instant => // ignore
              }
            }

            val dependenciesStream = Flux.combineLatest(dependencies, (arr: Array[AnyRef]) => arr)

            val exhaustivePath = dependenciesStream.map{booksStats =>
              map(targetPath, initialAmount, fee, path, booksStats)
            }

            exhaustivePath
          }

          val pathsStream = pathsIterable.toSeq

          Flux.just(Flux.empty[ExhaustivePath], pathsStream: _*)
            .skip(1)
            .flatMap(identity, pathsStream.length)
        }

        def map(targetPath: TargetPath, startAmount: Amount, fee: FeeMultiplier, path: List[(PathOrderType, Market)], booksStats: Array[AnyRef]): ExhaustivePath = {
          val chain = new ListBuffer[InstantDelayedOrder]()
          var targetCurrency = targetPath._1
          var fromAmount = startAmount

          var i = 0

          for {
            (tpe, market) <- path
          } {
            targetCurrency = market.other(targetCurrency).get
            val orderBook = booksStats(i).asInstanceOf[OrderBookAbstract]
            i += 1

            val order = tpe match {
              case PathOrderType.Instant =>
                mapInstantOrder(market, targetCurrency, fromAmount, fee.taker, orderBook)
              case PathOrderType.Delayed =>
                val stat = booksStats(i).asInstanceOf[TradeStat]
                i += 1
                mapDelayedOrder(market, targetCurrency, fromAmount, fee.maker, orderBook, stat)
            }

            fromAmount = order.toAmount

            chain += order
          }

          ExhaustivePath(targetPath, chain.toList)
        }

        private def mapInstantOrder(market: Market, targetCurrency: Currency, fromAmount: Amount, takerFee: BigDecimal, orderBook: OrderBookAbstract): Orders.InstantOrder = {
          Orders.getInstantOrder(market, targetCurrency, fromAmount, takerFee, orderBook).get
        }

        private def mapDelayedOrder(market: Market, targetCurrency: Currency, fromAmount: Amount, makerFee: BigDecimal, orderBook: OrderBookAbstract, stat: TradeStat): Orders.DelayedOrder = {
          val stat0 = statOrder(market, targetCurrency, stat)
          Orders.getDelayedOrder(market, targetCurrency, fromAmount, makerFee, orderBook, stat0).get
        }

        private def statOrder(market: Market, targetCurrency: Currency, stat: TradeStat): TradeStatOrder = {
          market.orderType(targetCurrency).get match {
            case OrderType.Buy => stat.buy
            case OrderType.Sell => stat.sell
          }
        }
      }

      object ExhaustivePathOrdering extends Ordering[ExhaustivePath] {
        override def compare(x: ExhaustivePath, y: ExhaustivePath): Int = {
          x.simpleMultiplier.compare(y.simpleMultiplier)
        }
      }
    }
  }

  def start(): Flux[Unit] = {
    logger.info("Start trading")

    indicators.paths.subscribe(x => {
      println("state received")
    })

    Flux.just(())
  }
}

object Trader {
  class OpenOrder(
    val id: Long,
    val tpe: OrderType,
    val market: Market,
    val rate: Price,
    val amount: Amount,
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
