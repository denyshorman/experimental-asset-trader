package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi._
import com.typesafe.scalalogging.Logger
import reactor.core.Disposable
import reactor.core.publisher.{FluxSink, ReplayProcessor}
import reactor.core.scala.publisher.{Flux, FluxProcessor}
import reactor.core.scheduler.Scheduler

import scala.collection.mutable

class Trader(private val poloniexApi: PoloniexApi)(implicit val vertxScheduler: Scheduler) {
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
      triggerStream.startWith(()).flatMap(_ => stream).replay(1).refCount()
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

  private object data {
    type MarketIntMap = mutable.Map[Int, Market]
    type MarketStringMap = mutable.Map[Market, Int]
    type MarketData = (MarketIntMap, MarketStringMap)

    val currencies: Flux[(Map[Currency, CurrencyDetails], Map[Int, Currency])] = {
      raw.currencies.map(curr => {
        (curr, curr.map(kv => (kv._2.id, kv._1)))
      }).replay(1).refCount()
    }

    val balances: Flux[mutable.Map[Currency, BigDecimal]] = {
      val balancesStream = raw.balances.map(b => mutable.Map(b.toSeq: _*))
      val balanceChangesStream = raw.accountNotifications.filter(_.isInstanceOf[BalanceUpdate]).map(_.asInstanceOf[BalanceUpdate])

      val updatedBalance = balanceChangesStream.withLatestFrom(balancesStream, (n, b: mutable.Map[Currency, BigDecimal]) => (n, b)).flatMap { case (balanceUpdate, allBalances) => currencies.map(curr => {
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
      })
      }

      updatedBalance.replay(1).refCount()
    }

    val markets: Flux[MarketData] = {
      raw.ticker.map(tickers => {
        val marketIntStringMap: MarketIntMap = mutable.Map()
        val marketStringIntMap: MarketStringMap = mutable.Map()

        tickers.foreach(tick => {
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
      })
      val limitOrderCreatedStream = poloniexApi.accountNotificationStream.filter(_.isInstanceOf[LimitOrderCreated]).map(_.asInstanceOf[LimitOrderCreated])
      val orderUpdateStream = poloniexApi.accountNotificationStream.filter(_.isInstanceOf[OrderUpdate]).map(_.asInstanceOf[OrderUpdate])

      val ordersLimitOrderUpdate = limitOrderCreatedStream.withLatestFrom(initialOrdersStream, (limitOrderCreated, orders: mutable.Set[Trader.OpenOrder]) => {
        logger.info(s"Account info: $limitOrderCreated")

        markets.map(market => {
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
      }).flatMap(o => o)

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
      val allTickersStream = raw.ticker.map(allTickers => mutable.Map(allTickers.toSeq: _*))

      val tickersUpdate = raw.tickerStream.withLatestFrom(allTickersStream, (ticker, allTickers: mutable.Map[Market, Ticker]) => {
        markets.map(market => {
          val marketId = market._1.get(ticker.id)

          if (marketId.isDefined) {
            allTickers.put(marketId.get, ticker)
            logger.whenDebugEnabled {
              logger.debug(ticker.toString)
            }
          } else {
            logger.warn("Market not found in local market cache. Updating market cache...")
            trigger.ticker.onNext(())
          }

          allTickers
        })
      }).flatMap(o => o)

      tickersUpdate.replay(1).refCount()
    }
  }

  def start(): Flux[Unit] = {
    Flux.create((sink: FluxSink[Unit]) => {
      val currencies = data.currencies.subscribe(currencies => {}, error => {})
      val balances = data.balances.subscribe(balances => {}, error => {})
      val markets = data.markets.subscribe(markets => {}, error => {})
      val openOrders = data.openOrders.subscribe(orders => {}, error => {})
      val tickers = data.tickers.subscribe(tickers => {}, error => {})

      val disposable = new Disposable {
        override def dispose(): Unit = {
          currencies.dispose()
          balances.dispose()
          markets.dispose()
          openOrders.dispose()
          tickers.dispose()
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
}