package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.Main
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{Currency, CurrencyDetails}
import com.typesafe.scalalogging.Logger
import io.vertx.lang.scala.ScalaVerticle
import reactor.core.scala.publisher.Mono

import scala.concurrent.{Future, Promise}

class Trader(
  private val poloniexApi: PoloniexApi
) extends ScalaVerticle {
  private val logger = Logger[Trader]
  private var allCurrencies: Map[Currency, CurrencyDetails] = _

  private def init(): Mono[Unit] = {
    poloniexApi.currencies().subscribe(curr => {
      allCurrencies = curr
    })

    Mono.empty
  }

  override def startFuture(): Future[Unit] = {
    logger.info("Start trading")
    Future.successful(())
  }

  override def stopFuture(): Future[Unit] = {
    logger.info("Stop trading")
    Future.successful(())
  }
}
