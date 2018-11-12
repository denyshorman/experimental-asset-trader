package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{Currency, CurrencyDetails}
import com.typesafe.scalalogging.Logger
import io.vertx.lang.scala.ScalaVerticle
import reactor.core.scala.publisher.Mono

import scala.concurrent.Future

class Trader(
  private val poloniexApi: PoloniexApi
) extends ScalaVerticle {
  private val logger = Logger[Trader]
  private var allCurrencies: Map[Currency, CurrencyDetails] = _
  private var allBalances: Map[Currency, BigDecimal] = _

  private def init(): Mono[Unit] = {
    poloniexApi.currencies().subscribe(curr => {
      logger.info("All currencies fetched")
      allCurrencies = curr
    })

    poloniexApi.balances().subscribe(balances => {
      logger.info("All available balances fetched")
      allBalances = balances
    })

    Mono.empty
  }

  override def startFuture(): Future[Unit] = {
    init()
    logger.info("Start trading")
    Future.successful(())
  }

  override def stopFuture(): Future[Unit] = {
    logger.info("Stop trading")
    Future.successful(())
  }
}
