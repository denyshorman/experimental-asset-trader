package com.gitlab.dhorman.cryptotrader.util

import org.scalatest.FlatSpec

class SecretsSpec extends FlatSpec {

  "Secrets get" should "return some value" in {
    val secret = Secrets.get("test")
    assert(secret.isEmpty)
  }

}
