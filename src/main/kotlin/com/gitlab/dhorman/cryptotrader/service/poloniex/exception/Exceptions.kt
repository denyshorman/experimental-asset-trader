package com.gitlab.dhorman.cryptotrader.service.poloniex.exception

import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import java.math.BigDecimal

open class PoloniexException(open val originalMsg: String) : Throwable(originalMsg, null, true, false)

class IncorrectNonceException(val providedNonce: Long, val requiredNonce: Long, override val originalMsg: String) :
    PoloniexException(originalMsg)

class ApiCallLimitException(val maxRequestPerSecond: Int, override val originalMsg: String) :
    PoloniexException(originalMsg)

open class AmountMustBeAtLeastException(val quoteAmount: Amount, override val originalMsg: String) :
    PoloniexException(originalMsg)

class TotalMustBeAtLeastException(val totalAmount: Amount, override val originalMsg: String) :
    PoloniexException(originalMsg)

class RateMustBeLessThanException(val maxRate: Amount, override val originalMsg: String) :
    PoloniexException(originalMsg)

class MaxOrdersExceededException(val maxOrders: Int, override val originalMsg: String) :
    PoloniexException(originalMsg)

class NotEnoughCryptoException(val currency: Currency, override val originalMsg: String) :
    PoloniexException(originalMsg)

class OrderCompletedOrNotExistException(val orderId: Long, override val originalMsg: String) :
    PoloniexException(originalMsg)

object DisconnectedException : PoloniexException("Websocket connection to Poloniex server has been closed")
object SubscribeErrorException : PoloniexException("")
object InvalidOrderNumberException : PoloniexException(InvalidOrderNumberMsg)
object TransactionFailedException : PoloniexException(TransactionFailedMsg)
object UnableToFillOrderException : PoloniexException(UnableToFillOrderMsg)
object UnableToPlacePostOnlyOrderException : PoloniexException(UnableToPlacePostOnlyOrderMsg)
object AlreadyCalledMoveOrderException : PoloniexException(AlreadyCalledMoveOrderMsg)
object PermissionDeniedException : PoloniexException(PermissionDeniedMsg)
object InvalidChannelException : PoloniexException(InvalidChannelMsg)
object AmountIsZeroException : AmountMustBeAtLeastException(BigDecimal.ZERO, "Quote amount is zero")
object MarketDisabledException : PoloniexException(MarketDisabledMsg)
object InvalidMarketException : PoloniexException(InvalidMarketMsg)
object InvalidDepthException : PoloniexException(InvalidDepthMsg)
object OrderMatchingDisabledException : PoloniexException(OrderMatchingDisabledMsg)
object AlreadyCalledCancelOrMoveOrderException : PoloniexException(AlreadyCalledCancelOrMoveOrderMsg)
object PoloniexInternalErrorException : PoloniexException(InternalErrorMsg)
