package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{PoloniexApiKeyTag, PoloniexApiSecretTag}
import com.gitlab.dhorman.cryptotrader.util.Secrets
import com.softwaremill.macwire._
import com.softwaremill.tagging._
import io.vertx.core.{Vertx => JavaVertx}
import io.vertx.reactivex.RxHelper
import io.vertx.scala.core.Vertx
import reactor.adapter.rxjava.RxJava2Scheduler
import reactor.core.scheduler.Scheduler

trait MainModule {
  implicit val vertx: Vertx

  implicit lazy val vertxScheduler: Scheduler = RxJava2Scheduler.from(RxHelper.scheduler(vertx.asJava.asInstanceOf[JavaVertx]))

  lazy val poloniexApiKey: String @@ PoloniexApiKeyTag = Secrets.get("POLONIEX_API_KEY")
    .getOrElse({throw new Exception("Please define POLONIEX_API_KEY environment variable")})
    .taggedWith[PoloniexApiKeyTag]

  lazy val poloniexApiSecret: String @@ PoloniexApiSecretTag = Secrets.get("POLONIEX_API_SECRET")
    .getOrElse({throw new Exception("Please define POLONIEX_API_SECRET environment variable")})
    .taggedWith[PoloniexApiSecretTag]

  lazy val poloniexApi: PoloniexApi = wire[PoloniexApi]
}
