package com.gitlab.dhorman.cryptotrader.service

import com.gitlab.dhorman.cryptotrader.TestModule
import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

class PoloniexApiSpec extends FlatSpec with TestModule {
  private val logger = Logger[PoloniexApiSpec]

  "PoloniexApi ticker stream" should "return some amount of data and completes successfully" in {
    val p = Promise[Unit]()

    poloniexApi.tickerStream.take(10)
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(ticker => {
        logger.info(ticker.toString)
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi _24HourExchangeVolumeStream" should "return one 24 hours exchange volume" in {
    val p = Promise[Unit]()

    poloniexApi._24HourExchangeVolumeStream.take(1)
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(_24HVolume => {
        logger.info(_24HVolume.toString)
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi accountNotificationStream" should "return account related notifications" in {
    val p = Promise[Unit]()

    poloniexApi.accountNotificationStream
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(notification => {
        logger.info(notification.toString)
      }, err => {
        err.printStackTrace()
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi returnBalances" should "return some value" in {
    val p = Promise[Unit]()

    poloniexApi.balances()
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(balances => {
        logger.info(balances.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi returnCompleteBalances" should "return some value" in {
    val p = Promise[Unit]()

    poloniexApi.completeBalances()
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(balances => {
        logger.info(balances.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi returnDepositAddresses" should "return some value" in {
    val p = Promise[Unit]()

    poloniexApi.depositAddresses()
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(address => {
        logger.info(address.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi openOrders" should "return some value" in {
    val p = Promise[Unit]()

    poloniexApi.openOrders("ETH_OMG")
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(address => {
        logger.info(address.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi allOpenOrders" should "return some value" in {
    val p = Promise[Unit]()

    poloniexApi.allOpenOrders()
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(address => {
        logger.info(address.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }

  "PoloniexApi currencies" should "return all currencies available" in {
    val p = Promise[Unit]()

    poloniexApi.currencies()
      .doOnTerminate(() => {
        p.success(())
      })
      .subscribe(currencies => {
        logger.info(currencies.toString())
      }, err => {
        logger.error(err.toString)
        fail(err)
      })

    Await.result(p.future, Duration.Inf)
  }
}
