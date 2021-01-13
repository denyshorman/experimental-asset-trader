package com.gitlab.dhorman.cryptotrader.trader.exception

import com.gitlab.dhorman.cryptotrader.core.SimulatedPath
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import io.vavr.collection.Array

abstract class NotProfitableException(override val message: String) : Throwable(message, null, true, false)

class NotProfitableDeltaException(val fromAmount: Amount, val targetAmount: Amount) :
    NotProfitableException("Not profitable: $targetAmount - $fromAmount = ${targetAmount - fromAmount}")

data class NewProfitablePathFoundException(val oldPath: Array<TranIntentMarket>, val newPath: SimulatedPath) :
    NotProfitableException("New profitable path found $newPath for $oldPath")

object NotProfitableTimeoutException : NotProfitableException("Not profitable due to long waiting time")

object MoveNotRequiredException : Throwable("", null, true, false)
object CantMoveOrderSafelyException : Throwable("", null, true, false)
object RepeatPlaceMoveOrderLoopAgain : Throwable("", null, true, false)
object CompletePlaceMoveOrderLoop : Throwable("", null, true, false)
