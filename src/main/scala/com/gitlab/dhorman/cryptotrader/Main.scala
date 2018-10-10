package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.typesafe.scalalogging.Logger
import io.vertx.reactivex.core.Vertx


object Main extends App {
  val vertx: Vertx = Vertx.vertx()
  val logger = Logger[Main.type]

  val p = new PoloniexApi(vertx)

  p.tickerStream.take(10)
    .doOnTerminate(() => {
      //vertx.close()
    })
    .subscribe(ticker => {
      logger.info("1: " + ticker.toString)
    }, err => {
      logger.error(err.toString)
    }, () => {
      logger.info("ticker stream completed")
    })
}
