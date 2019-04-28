package com.gitlab.dhorman.cryptotrader.core

class OrderBookEmptyException(bookType: SubBookType) : Throwable("Order book $bookType is empty")

enum class SubBookType {Buy, Sell}