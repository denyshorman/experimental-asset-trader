package com.gitlab.dhorman.cryptotrader

import com.gitlab.dhorman.cryptotrader.service.PoloniexApi
import com.gitlab.dhorman.cryptotrader.service.PoloniexApi.{PoloniexApiKeyTag, PoloniexApiSecretTag}
import com.gitlab.dhorman.cryptotrader.util.Secrets
import com.softwaremill.macwire._
import com.softwaremill.tagging._
import io.vertx.scala.core.Vertx

trait MainModule {
  val vertx: Vertx

  lazy val poloniexApiKey: String @@ PoloniexApiKeyTag = Secrets.get("POLONIEX_API_KEY")
    .getOrElse({throw new Exception("Please define POLONIEX_API_KEY environment variable")})
    .taggedWith[PoloniexApiKeyTag]

  lazy val poloniexApiSecret: String @@ PoloniexApiSecretTag = Secrets.get("POLONIEX_API_SECRET")
    .getOrElse({throw new Exception("Please define POLONIEX_API_SECRET environment variable")})
    .taggedWith[PoloniexApiSecretTag]

  lazy val poloniexApi: PoloniexApi = wire[PoloniexApi]
}
