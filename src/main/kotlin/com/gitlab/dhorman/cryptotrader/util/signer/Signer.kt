package com.gitlab.dhorman.cryptotrader.util.signer

interface Signer {
    fun sign(msg: String): String
}
