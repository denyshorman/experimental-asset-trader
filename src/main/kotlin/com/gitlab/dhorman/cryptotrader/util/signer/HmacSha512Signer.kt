package com.gitlab.dhorman.cryptotrader.util.signer

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class HmacSha512Signer(key: String, private val convertToString: ByteArray.() -> String) : Signer {
    private val signingKey: SecretKeySpec
    private val macInstance: Mac

    init {
        val algorithm = "HmacSHA512"
        signingKey = SecretKeySpec(key.toByteArray(), algorithm)
        macInstance = Mac.getInstance(algorithm)
        macInstance.init(signingKey)
    }

    override fun sign(msg: String): String {
        val mac = macInstance.clone() as Mac
        return mac.doFinal(msg.toByteArray()).convertToString()
    }
}
