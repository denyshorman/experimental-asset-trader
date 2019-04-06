package com.gitlab.dhorman.cryptotrader.service.poloniex.exception

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount

data class IncorrectNonceException(
    val providedNonce: Long,
    val requiredNonce: Long,
    val originalMsg: String
) : Throwable(originalMsg, null, true, false)

data class ApiCallLimitException(
    val maxRequestPerSecond: Int,
    val originalMsg: String
) : Throwable(originalMsg, null, true, false)

data class TotalMustBeAtLeastException(
    val totalAmount: Amount,
    val originalMsg: String
) : Throwable(originalMsg, null, true, false)

data class RateMustBeLessThanException(
    val maxRate: Amount,
    val originalMsg: String
) : Throwable(originalMsg, null, true, false)