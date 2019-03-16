package com.gitlab.dhorman.cryptotrader.util

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class HmacSha512Digest(key: String) {
    private val signingKey = SecretKeySpec(key.toByteArray(), HMAC_SHA512_ALGORITHM)

    fun sign(msg: String): String {
        val mac = Mac.getInstance(HMAC_SHA512_ALGORITHM)
        mac.init(signingKey)
        return mac.doFinal(msg.toByteArray()).toHexString()
    }

    companion object {
        private const val HMAC_SHA512_ALGORITHM = "HmacSHA512"
    }
}