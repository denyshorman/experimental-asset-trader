package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.exception

val IncorrectNonceMsgPattern = """Nonce must be greater than (\d+)\. You provided (\d+)\.""".toRegex()
val ApiCallLimitPattern = """Please do not make more than (\d+) API calls per second\.""".toRegex()
val AmountMustBeAtLeastPattern = """Amount must be at least (\d+(\.\d+)?)\.""".toRegex()
val TotalMustBeAtLeastPattern = """Total must be at least (\d+(\.\d+)?)\.""".toRegex()
val RateMustBeLessThanPattern = """Rate must be less than (\d+(\.\d+)?)\.""".toRegex()
val OrdersCountExceededPattern = """You may not have more than (\d+) open orders in a single market\.""".toRegex()
val NotEnoughCryptoPattern = """Not enough (.+?)\.""".toRegex()
const val InvalidOrderNumberMsg = """Invalid order number, or you are not the person who placed the order."""
const val TransactionFailedMsg = """Transaction failed. Please try again."""
const val OrderNotFoundPattern = """Order not found, or you are not the person who placed it."""
const val UnableToFillOrderMsg = """Unable to fill order completely."""
const val UnableToPlacePostOnlyOrderMsg = """Unable to place post-only order at this price."""
const val AlreadyCalledMoveOrderMsg = """You have already called cancelOrder or moveOrder on this order. Please wait for that call's response."""
const val PermissionDeniedMsg = """Permission denied."""
const val InvalidChannelMsg = """Invalid channel."""
const val MarketDisabledMsg = """Market is disabled."""
const val InvalidMarketMsg = """Invalid currency pair."""
const val InvalidDepthMsg = """Invalid depth."""
const val OrderMatchingDisabledMsg = """Order matching is currently disabled."""
const val AlreadyCalledCancelOrMoveOrderMsg = """You have already called cancelOrder or moveOrder on this order. Please wait for that call's response."""
const val InternalErrorMsg = """Internal error. Please try again."""
const val MaintenanceModeMsg = """Currently in maintenance mode."""
val OrderCompletedOrNotExistPattern = """Order (\d+) is either completed or does not exist\.""".toRegex()
val OrderWithClientIdCompletedOrNotExistPattern = """Order with clientOrderId (\d+) is either completed or does not exist\.""".toRegex()
