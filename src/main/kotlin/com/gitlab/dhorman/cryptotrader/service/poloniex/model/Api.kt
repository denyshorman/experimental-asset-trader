package com.gitlab.dhorman.cryptotrader.service.poloniex.model

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.gitlab.dhorman.cryptotrader.core.Market
import com.gitlab.dhorman.cryptotrader.service.poloniex.codec.BooleanStringNumberJsonCodec
import io.vavr.collection.Array
import io.vavr.collection.Map
import java.math.BigDecimal
import java.time.LocalDateTime

data class Error(
    @JsonProperty("error") val msg: String?
)

data class Ticker0(
    val id: Int,
    val last: BigDecimal,
    val lowestAsk: BigDecimal,
    val highestBid: BigDecimal,
    val percentChange: BigDecimal,
    val baseVolume: BigDecimal,
    val quoteVolume: BigDecimal,

    @JsonSerialize(using = BooleanStringNumberJsonCodec.Encoder::class)
    @JsonDeserialize(using = BooleanStringNumberJsonCodec.Decoder::class)
    val isFrozen: Boolean,
    val high24hr: BigDecimal,
    val low24hr: BigDecimal
)

data class CompleteBalance(
    val available: BigDecimal,
    val onOrders: BigDecimal,
    val btcValue: BigDecimal
)

data class OpenOrder(
    val orderNumber: Long,
    val type: OrderType,
    @JsonProperty("rate") val price: Price,
    val startingAmount: Amount,
    val amount: Amount,
    val total: BigDecimal,
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss") val date: LocalDateTime,
    val margin: Boolean
)

data class TradeHistory(
    @JsonProperty("globalTradeID") val globalTradeId: Long,
    @JsonProperty("tradeID") val tradeId: Long,
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss") val date: LocalDateTime,
    val type: OrderType,
    @JsonProperty("rate") val price: Price,
    val amount: Amount,
    val total: BigDecimal,
    val orderNumber: Long
)

data class CurrencyDetails(
    val id: Int,
    val name: String,
    val humanType: String,
    val currencyType: String?,
    val txFee: BigDecimal,
    val minConf: BigDecimal,
    val depositAddress: String?,
    val disabled: Boolean,
    val delisted: Boolean,
    val frozen: Boolean,
    @get:JsonProperty("isGeofenced") val isGeofenced: Boolean
)

//TODO: Verify values
enum class TradeCategory(@get:JsonValue val id: String) {
    Exchange("exchange"),
    Margin("margin"),
}

data class TradeHistoryPrivate(
    @JsonProperty("globalTradeID") val globalTradeId: Long,
    @JsonProperty("tradeID") val tradeId: Long,
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss") val date: LocalDateTime,
    @JsonProperty("rate") val price: Price,
    val amount: BigDecimal,
    val total: BigDecimal,
    val fee: BigDecimal,
    val orderNumber: Long,
    val type: OrderType,
    val category: TradeCategory
)

data class OrderTrade(
    @JsonProperty("globalTradeID") val globalTradeId: Long,
    @JsonProperty("tradeID") val tradeId: Long,
    @JsonProperty("currencyPair") val market: Market,
    val type: OrderType,
    @JsonProperty("rate") val price: Price,
    val amount: Amount,
    val total: BigDecimal,
    val fee: BigDecimal,
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss") val date: LocalDateTime
)


data class AvailableAccountBalance(
    val exchange: Map<Currency, Amount>,
    val margin: Map<Currency, Amount>,
    val lending: Map<Currency, Amount>
)

data class Buy(
    val orderNumber: Long,
    val resultingTrades: Array<BuyResultingTrade>
)

data class BuyResultingTrade(
    @JsonProperty("tradeID") val tradeId: Long,
    val type: OrderType,
    @JsonProperty("rate") val price: Price,
    val amount: Amount,
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss") val date: LocalDateTime,
    val total: BigDecimal
)

enum class BuyOrderType(@get:JsonValue val id: String) {
    // A "fill or kill" order is a limit order that must be filled immediately in its entirety or it is canceled (killed). The purpose of a fill or kill order is to ensure that a position is entered instantly and at a specific price.
    FillOrKill("fillOrKill"),

    // An Immediate Or Cancel (IOC) order requires all or part of the order to be executed immediately, and any unfilled parts of the order are canceled. Partial fills are accepted with this type of order duration, unlike a fill-or-kill order, which must be filled immediately in its entirety or be canceled.
    ImmediateOrCancel("immediateOrCancel"),

    // https://support.bitfinex.com/hc/en-us/articles/115003507365-Post-Only-Limit-Order-Option
    // The post-only limit order option ensures the limit order will be added to the order book and not match with a pre-existing order. If your order would cause a match with a pre-existing order, your post-only limit order will be canceled. This ensures that you will pay the maker fee and not the taker fee. Visit the fees page for more information.
    PostOnly("postOnly")
}

data class CancelOrder(
    val success: Boolean,
    val amount: Amount,
    val message: String
)

data class MoveOrderResult(
    val success: Boolean,
    val orderNumber: Long,
    val amount: Amount,
    val message: String,
    val resultingTrades: Map<Market, Array<BuyResultingTrade>>
)

data class FeeInfo(
    val makerFee: BigDecimal,
    val takerFee: BigDecimal,
    val thirtyDayVolume: BigDecimal,
    val nextTier: BigDecimal
)

enum class AccountType(@get:JsonValue val id: String) {
    Exchange("exchange"),
    Margin("margin")
}

