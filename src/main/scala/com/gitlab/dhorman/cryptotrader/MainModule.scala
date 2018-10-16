package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{PoloniexApiKeyTag, PoloniexApiSecretTag}
import com.gitlab.dhorman.cryptotrader.util.Secrets
import com.softwaremill.macwire._
import com.softwaremill.tagging._
import io.vertx.core.Vertx
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.reactivex.core.{Vertx => VertxRx}
import io.vertx.scala.core.{Vertx => VertxScala}

import scala.concurrent.ExecutionContext

trait MainModule {
  lazy val vertx: Vertx = Vertx.vertx()
  lazy val vertxRx = new VertxRx(vertx)
  lazy val scalaVertx = VertxScala(vertx)
  implicit lazy val ec: ExecutionContext = VertxExecutionContext(scalaVertx.getOrCreateContext())

  lazy val poloniexApiKey: String @@ PoloniexApiKeyTag = Secrets.get("POLONIEX_API_KEY")
    .getOrElse({throw new Exception("Please define POLONIEX_API_KEY environment variable")})
    .taggedWith[PoloniexApiKeyTag]

  lazy val poloniexApiSecret: String @@ PoloniexApiSecretTag = Secrets.get("POLONIEX_API_SECRET")
    .getOrElse({throw new Exception("Please define POLONIEX_API_SECRET environment variable")})
    .taggedWith[PoloniexApiSecretTag]

  lazy val poloniexApi: PoloniexApi = wire[PoloniexApi]
}
