package com.gitlab.dhorman.cryptotrader.service.poloniex.exception

val IncorrectNonceMsgPattern = """Nonce must be greater than (\d+)\. You provided (\d+)\.""".toRegex()
val ApiCallLimitPattern = """Please do not make more than (\d+) API calls per second\.""".toRegex()
val TotalMustBeAtLeastPattern = """Total must be at least (\d+(\.\d+)?)\.""".toRegex()
val RateMustBeLessThanPattern = """Rate must be less than (\d+(\.\d+)?)\.""".toRegex()
val OrdersCountExceededPattern = """You may not have more than (\d+) open orders in a single market\.""".toRegex()
val NotEnoughCryptoPattern = """Not enough (.+?)\.""".toRegex()
const val InvalidOrderNumberPattern = """Invalid order number, or you are not the person who placed the order."""
const val TransactionFailedPattern = """Transaction failed. Please try again."""
const val OrderNotFoundPattern = """Order not found, or you are not the person who placed it."""
const val UnableToFillOrderPattern = """Unable to fill order completely."""
const val UnableToPlacePostOnlyOrderPattern = """Unable to place post-only order at this price."""
val OrderCompletedOrNotExistPattern = """Order (\d+) is either completed or does not exist\.""".toRegex()
