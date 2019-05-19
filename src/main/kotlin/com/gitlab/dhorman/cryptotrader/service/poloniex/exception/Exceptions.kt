package com.gitlab.dhorman.cryptotrader.service.poloniex.exception

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency

open class PoloniexException(open val originalMsg: String) : Throwable(originalMsg, null, true, false)

data class IncorrectNonceException(val providedNonce: Long, val requiredNonce: Long, override val originalMsg: String) :
    PoloniexException(originalMsg)

data class ApiCallLimitException(val maxRequestPerSecond: Int, override val originalMsg: String) :
    PoloniexException(originalMsg)

data class TotalMustBeAtLeastException(val totalAmount: Amount, override val originalMsg: String) :
    PoloniexException(originalMsg)

data class RateMustBeLessThanException(val maxRate: Amount, override val originalMsg: String) :
    PoloniexException(originalMsg)

data class MaxOrdersExceededException(val maxOrders: Int, override val originalMsg: String) :
    PoloniexException(originalMsg)

data class NotEnoughCryptoException(val currency: Currency, override val originalMsg: String) :
    PoloniexException(originalMsg)

object InvalidOrderNumberException : PoloniexException(InvalidOrderNumberPattern)
object TransactionFailedException : PoloniexException(TransactionFailedPattern)