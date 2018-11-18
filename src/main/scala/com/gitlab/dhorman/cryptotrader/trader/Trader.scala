package com.gitlab.dhorman.cryptotrader.trader

import java.util.concurrent.TimeUnit

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{Currency, CurrencyDetails}
import com.typesafe.scalalogging.Logger
import reactor.core.scala.publisher.{Flux, Mono}
import reactor.core.scheduler.Scheduler

import scala.concurrent.duration.Duration

class Trader(private val poloniexApi: PoloniexApi)(implicit val vertxScheduler: Scheduler) {
  private val logger = Logger[Trader]
  private var allCurrencies: Map[Currency, CurrencyDetails] = _
  private var allBalances: Map[Currency, BigDecimal] = _

  def start(): Unit = {
    logger.info("Start trading")

    poloniexApi.currencies().subscribe(curr => {
      logger.info("All currencies fetched")
      allCurrencies = curr
    })

    Flux.interval(Duration(500, TimeUnit.MILLISECONDS), vertxScheduler).subscribe(value => {
      poloniexApi.balances().subscribe(balances => {
        logger.info("All available balances fetched")
        allBalances = balances
      })
    })
  }

  def stop(): Mono[Unit] = {
    logger.info("Stop trading")
    Mono.empty
  }
}
