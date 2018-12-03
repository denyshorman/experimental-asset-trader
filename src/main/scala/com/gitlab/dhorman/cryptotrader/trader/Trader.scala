package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi._
import com.typesafe.scalalogging.Logger
import reactor.core.scala.publisher.Mono
import reactor.core.scheduler.Scheduler

import scala.collection.mutable

class Trader(private val poloniexApi: PoloniexApi)(implicit val vertxScheduler: Scheduler) {
  private val logger = Logger[Trader]
  private var currencies: mutable.Map[Currency, CurrencyDetails] = mutable.Map()
  private var currenciesIntStringMap: mutable.Map[Int, Currency] = mutable.Map()
  private var marketIntStringMap: mutable.Map[Int, Market] = mutable.Map() // TODO: How to get this mapping ?
  private var marketStringIntMap: mutable.Map[Market, Int] = mutable.Map() // TODO: How to get this mapping ?
  private var allBalances: mutable.Map[Currency, BigDecimal] = mutable.Map()
  private var openOrders: mutable.Set[Trader.OpenOrder] = mutable.Set()
  private var tickers: mutable.Map[Market, Ticker] = mutable.Map()

  private def sync(): Unit = {
    poloniexApi.currencies().subscribe(curr => {
      logger.info("All currencies fetched")
      currencies = mutable.Map(curr.toSeq: _*)
      currenciesIntStringMap = currencies.map(kv => (kv._2.id, kv._1))
      logger.whenDebugEnabled {
        logger.debug(currencies.toString)
      }
    })

    poloniexApi.balances().subscribe(balances => {
      logger.info("All available balances fetched")
      allBalances = mutable.Map(balances.toSeq: _*)
      logger.whenDebugEnabled {
        logger.debug(allBalances.toString)
      }
    })

    poloniexApi.allOpenOrders().subscribe(orders => {
      logger.info("All open orders fetched")
      openOrders = orders.flatMap(kv => kv._2.view.map(o => new Trader.OpenOrder(o.id, o.tpe, kv._1, o.rate, o.amount))).to[mutable.Set]
      logger.whenDebugEnabled {
        logger.debug(openOrders.toString)
      }
    })

    poloniexApi.ticker().subscribe((ticker: Map[Market, Ticker]) => {
      tickers = mutable.Map(ticker.toSeq: _*)
      tickers.foreach(tick => {
        marketIntStringMap.put(tick._2.id, tick._1)
        marketStringIntMap.put(tick._1, tick._2.id)
      })

      logger.whenDebugEnabled {
        logger.debug(s"Initial ticker dump: ${tickers.toString}")
      }

      poloniexApi.tickerStream.subscribe((ticker: Ticker) => {
        val marketId = marketIntStringMap.get(ticker.id)

        if (marketId.isDefined) {
          tickers.put(marketId.get, ticker)
          logger.whenDebugEnabled {
            logger.debug(ticker.toString)
          }
        } else {
          // TODO: Ticker not found in local cache. Update local cache.
          logger.warn("Ticker not found in local cache. Update local cache.")
        }
      })
    })

    poloniexApi.accountNotificationStream.subscribe(_ match {
      case balanceUpdate: BalanceUpdate =>
        logger.info(s"Account info: $balanceUpdate")

        if (balanceUpdate.walletType == WalletType.Exchange) {
          val currencyId = currenciesIntStringMap.get(balanceUpdate.currencyId)

          if (currencyId.isDefined) {
            val balance = allBalances.get(currencyId.get)

            if (balance.isDefined) {
              val newBalance = balance.get + balanceUpdate.amount
              allBalances(currencyId.get) = newBalance
            } else {
              // TODO: Balance not found in local cache. Update local balances.
              logger.warn("Balance not found in local cache. Update local balances.")
            }
          } else {
            // TODO: Currency not found in local cache. Update local cache
            logger.warn("Currency not found in local cache. Update local cache")
          }
        }
      case limitOrderCreated: LimitOrderCreated =>
        logger.info(s"Account info: $limitOrderCreated")

        val marketId = marketIntStringMap.get(limitOrderCreated.marketId)

        if (marketId.isDefined) {
          val newOrder = new Trader.OpenOrder(
            limitOrderCreated.orderNumber,
            limitOrderCreated.orderType,
            marketId.get,
            limitOrderCreated.rate,
            limitOrderCreated.amount,
          )

          openOrders += newOrder
        } else {
          // TODO: Market id not found in local cache. Refresh the cache
          logger.warn("Market id not found in local cache. Refresh the cache")
        }
      case orderUpdate: OrderUpdate =>
        logger.info(s"Account info: $orderUpdate")

        if (orderUpdate.newAmount == 0) {
          openOrders.remove(new Trader.OpenOrder(orderUpdate.orderId))
        } else {
          val order = openOrders.view.find(_.id == orderUpdate.orderId)

          if (order.isDefined) {
            order.get.amount = orderUpdate.newAmount
          } else {
            // TODO: Order not found in local cache. Update local cache.
            logger.warn("Order not found in local cache. Update local cache.")
          }
        }
      case tradeNotification: TradeNotification =>
        logger.info(s"Account info: $tradeNotification")

        if (tradeNotification.fundingType == FundingType.ExchangeWallet) {
          val order = openOrders.view.find(_.id == tradeNotification.orderId)

          if (order.isDefined) {
            /*tradeNotification.orderId
            tradeNotification.tradeId
            tradeNotification.amount
            tradeNotification.rate
            tradeNotification.fundingType
            tradeNotification.feeMultiplier*/

            // TODO: How to update order ?
            logger.warn("How to update order ?")
          } else {
            // TODO: Order not found in local cache. Update local cache.
            logger.warn("Order not found in local cache. Update local cache.")
          }
        } else {
          // TODO: Upgrade to use another funds
          logger.warn("Upgrade to use another funds")
        }
    }, err => {
      err.printStackTrace()
    })
  }

  def start(): Unit = {
    sync()
    logger.info("Start trading")
  }

  def stop(): Mono[Unit] = {
    logger.info("Stop trading")
    Mono.empty
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