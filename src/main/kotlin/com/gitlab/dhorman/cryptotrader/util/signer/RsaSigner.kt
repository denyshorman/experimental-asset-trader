package com.gitlab.dhorman.cryptotrader.util.signer

import java.security.PrivateKey
import java.security.Signature

class RsaSigner(privateKey: PrivateKey, private val convertToString: ByteArray.() -> String) : Signer {
    private val signatureInstance = Signature.getInstance("SHA256withRSA")

    init {
        signatureInstance.initSign(privateKey)
    }

    override fun sign(msg: String): String {
        val signature = signatureInstance.clone() as Signature
        signature.update(msg.toByteArray())
        return signature.sign().convertToString()
    }
}
