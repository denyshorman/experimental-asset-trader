package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{Currency, CurrencyDetails, Market}
import com.typesafe.scalalogging.Logger
import reactor.core.scala.publisher.Mono
import reactor.core.scheduler.Scheduler
import io.circe.generic.auto._
import io.circe.syntax._

class Trader(private val poloniexApi: PoloniexApi)(implicit val vertxScheduler: Scheduler) {
  private val logger = Logger[Trader]
  private var allCurrencies: Map[Currency, CurrencyDetails] = _
  private var allBalances: Map[Currency, BigDecimal] = _
  private var openOrders: Map[Market, List[PoloniexApi.OpenOrder]] = _

  private def sync(): Unit = {
    poloniexApi.currencies().subscribe(curr => {
      logger.info("All currencies fetched")
      allCurrencies = curr
      logger.whenDebugEnabled {
        logger.debug(curr.asJson.noSpaces)
      }
    })

    poloniexApi.balances().subscribe(balances => {
      logger.info("All available balances fetched")
      allBalances = balances
      logger.whenDebugEnabled {
        logger.debug(balances.asJson.noSpaces)
      }
    })

    poloniexApi.allOpenOrders().subscribe(orders => {
      logger.info("All open orders fetched")
      openOrders = orders
      logger.whenDebugEnabled {
        logger.debug(orders.toString)
      }
    })

    poloniexApi.tickerStream.subscribe(ticker => {
      logger.whenDebugEnabled {
        //logger.debug(s"Ticker: ${ticker.asJson.noSpaces}")
      }
    })

    poloniexApi.orderBookStream(224).subscribe(orderBook => {

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
