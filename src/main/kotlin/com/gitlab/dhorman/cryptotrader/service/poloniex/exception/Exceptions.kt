package com.gitlab.dhorman.cryptotrader.service.poloniex.exception

data class IncorrectNonceException(
    val providedNonce: Long,
    val requiredNonce: Long,
    val originalMsg: String
) : Throwable(originalMsg, null, true, false)

data class ApiCallLimitException(
    val maxRequestPerSecond: Int,
    val originalMsg: String
) : Throwable(originalMsg, null, true, false)