package com.gitlab.dhorman.cryptotrader.util

import org.junit.jupiter.api.Test

class SecretsSpec {
    @Test
    fun `"Secrets get" should "return some value"`() {
        val secret = Secrets.get("test")
        assert(secret == null)
    }
}
