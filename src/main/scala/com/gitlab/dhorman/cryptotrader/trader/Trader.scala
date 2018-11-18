package com.gitlab.dhorman.cryptotrader.trader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{Currency, CurrencyDetails}
import com.typesafe.scalalogging.Logger
import reactor.core.scala.publisher.Mono

class Trader(private val poloniexApi: PoloniexApi) {
  private val logger = Logger[Trader]
  private var allCurrencies: Map[Currency, CurrencyDetails] = _
  private var allBalances: Map[Currency, BigDecimal] = _

  def start(): Unit = {
    logger.info("Start trading")

    poloniexApi.currencies().subscribe(curr => {
      logger.info("All currencies fetched")
      allCurrencies = curr
    })

    poloniexApi.balances().subscribe(balances => {
      logger.info("All available balances fetched")
      allBalances = balances
    })
  }

  def stop(): Mono[Unit] = {
    logger.info("Stop trading")
    Mono.empty
  }
}
