package com.gitlab.dhorman.cryptotrader.service

import com.typesafe.scalalogging.Logger
import io.vertx.reactivex.core.Vertx
import org.scalatest.FlatSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

class PoloniexApiSpec extends FlatSpec {
  private val logger = Logger[PoloniexApiSpec]

  "PoloniexApi ticker stream" should "return some amount of data and completes successfully" in {
    val vertx = Vertx.vertx()
    val api = new PoloniexApi(vertx)
    val p = Promise[Unit]()

    api.tickerStream.take(10)
      .doOnTerminate(() => {
        p.success()
      })
      .subscribe(ticker => {
        logger.info(ticker.toString)
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
    vertx.close()
  }

  "PoloniexApi _24HourExchangeVolumeStream" should "return one 24 hours exchange volume" in {
    val vertx = Vertx.vertx()
    val api = new PoloniexApi(vertx)
    val p = Promise[Unit]()

    api._24HourExchangeVolumeStream.take(1)
      .doOnTerminate(() => {
        p.success()
      })
      .subscribe(_24HVolume => {
        logger.info(_24HVolume.toString)
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
    vertx.close()
  }

  "PoloniexApi returnBalances" should "return some value" in {
    val vertx = Vertx.vertx()
    val api = new PoloniexApi(vertx)
    val p = Promise[Unit]()

    api.balances()
      .doOnTerminate(() => {
        p.success()
      })
      .subscribe(balances => {
        logger.info(balances.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
    vertx.close()
  }

  "PoloniexApi returnCompleteBalances" should "return some value" in {
    val vertx = Vertx.vertx()
    val api = new PoloniexApi(vertx)
    val p = Promise[Unit]()

    api.completeBalances()
      .doOnTerminate(() => {
        p.success()
      })
      .subscribe(balances => {
        logger.info(balances.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
    vertx.close()
  }

  "PoloniexApi returnDepositAddresses" should "return some value" in {
    val vertx = Vertx.vertx()
    val api = new PoloniexApi(vertx)
    val p = Promise[Unit]()

    api.depositAddresses()
      .doOnTerminate(() => {
        p.success()
      })
      .subscribe(address => {
        logger.info(address.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
    vertx.close()
  }

  "PoloniexApi openOrders" should "return some value" in {
    val vertx = Vertx.vertx()
    val api = new PoloniexApi(vertx)
    val p = Promise[Unit]()

    api.openOrders("ETH_OMG")
      .doOnTerminate(() => {
        p.success()
      })
      .subscribe(address => {
        logger.info(address.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
    vertx.close()
  }

  "PoloniexApi allOpenOrders" should "return some value" in {
    val vertx = Vertx.vertx()
    val api = new PoloniexApi(vertx)
    val p = Promise[Unit]()

    api.allOpenOrders()
      .doOnTerminate(() => {
        p.success()
      })
      .subscribe(address => {
        logger.info(address.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
    vertx.close()
  }
}
