package com.gitlab.dhorman.cryptotrader.service.poloniex.exception

val IncorrectNonceMsgPattern = """Nonce must be greater than (\d+)\. You provided (\d+)\.""".toRegex()
val ApiCallLimitPattern = """Please do not make more than (\d+) API calls per second\.""".toRegex()
val TotalMustBeAtLeastPattern = """Total must be at least (\d+(\.\d+)?)\.""".toRegex()
val RateMustBeLessThanPattern = """Rate must be less than (\d+(\.\d+)?)\.""".toRegex()
val OrdersCountExceededPattern = """You may not have more than (\d+) open orders in a single market\.""".toRegex()
const val InvalidOrderNumberPattern = """Invalid order number, or you are not the person who placed the order."""
const val TransactionFailedPattern = """Transaction failed. Please try again."""