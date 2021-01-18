package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec.*
import io.vavr.collection.Map
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime

@JsonFormat(shape = JsonFormat.Shape.ARRAY)
data class PushNotification(
    val channel: Int,
    val sequenceId: Long?,
    val data: JsonNode?
)

enum class CommandType(@get:JsonValue val id: String) {
    Subscribe("subscribe"),
    Unsubscribe("unsubscribe")
}

enum class DefaultChannel(@get:JsonValue val id: Int) {
    AccountNotifications(1000),
    TickerData(1002),
    DayExchangeVolume(1003),
    Heartbeat(1010)
}

data class Command(
    val command: CommandType,
    val channel: Int
)

data class PrivateCommand(
    val command: CommandType,
    val channel: Int,
    val key: String, // API key
    val payload: String, // nonce=<epoch ms>
    val sign: String
)


@JsonDeserialize(using = AccountNotificationJsonCodec.Decoder::class)
sealed class AccountNotification

@JsonDeserialize(using = BalanceUpdateJsonCodec.Decoder::class)
data class BalanceUpdate(
    val currencyId: Int,
    val walletType: WalletType,
    val amount: BigDecimal
) : AccountNotification()

@JsonDeserialize(using = LimitOrderCreatedJsonCodec.Decoder::class)
data class LimitOrderCreated(
    val marketId: Int,
    val orderId: Long,
    val orderType: OrderType,
    val price: BigDecimal,
    val amount: BigDecimal,
    val date: LocalDateTime,
    val originalAmountOrdered: BigDecimal,
    val clientOrderId: Long?
) : AccountNotification()

@JsonDeserialize(using = OrderUpdateJsonCodec.Decoder::class)
data class OrderUpdate(
    val orderId: Long,
    val newAmount: Amount,
    val orderType: OrderUpdateType,
    val clientOrderId: Long?,
    val originalAmountOrdered: BigDecimal
) : AccountNotification()

@JsonDeserialize(using = TradeNotificationJsonCodec.Decoder::class)
data class TradeNotification(
    val tradeId: Long,
    val price: Price,
    val amount: Amount,
    val feeMultiplier: BigDecimal,
    val fundingType: FundingType,
    val orderId: Long,
    val totalFee: BigDecimal,
    val date: LocalDateTime,
    val clientOrderId: Long?
) : AccountNotification()

@JsonDeserialize(using = OrderPendingAckJsonCodec.Decoder::class)
data class OrderPendingAck(
    val orderId: Long,
    val marketId: Int,
    val price: Price,
    val amount: Amount,
    val orderType: OrderType,
    val clientOrderId: Long?
) : AccountNotification()

@JsonDeserialize(using = OrderKilledJsonCodec.Decoder::class)
data class OrderKilled(
    val orderId: Long,
    val clientOrderId: Long?
) : AccountNotification()

@JsonDeserialize(using = OrderBookNotificationJsonCodec.Decoder::class)
sealed class OrderBookNotification

@JsonFormat(shape = JsonFormat.Shape.ARRAY)
@JsonDeserialize(using = JsonDeserializer.None::class)
data class OrderBookInit(
    val asks: Map<Price, Amount>,
    val bids: Map<Price, Amount>
) : OrderBookNotification()

@JsonDeserialize(using = OrderBookUpdateJsonCodec.Decoder::class)
data class OrderBookUpdate(
    val orderType: OrderType,
    val price: Price,
    val amount: Amount
) : OrderBookNotification()

@JsonDeserialize(using = OrderBookTradeJsonCodec.Decoder::class)
data class OrderBookTrade(
    val id: Long,
    val orderType: OrderType,
    val price: Price,
    val amount: Amount,
    val timestamp: Instant
) : OrderBookNotification()


@JsonFormat(shape = JsonFormat.Shape.ARRAY)
data class Ticker(
    val id: Int,
    val last: BigDecimal,
    val lowestAsk: BigDecimal,
    val highestBid: BigDecimal,
    val percentChange: BigDecimal,
    val baseVolume: BigDecimal,
    val quoteVolume: BigDecimal,
    val isFrozen: Boolean,
    val high24hr: BigDecimal,
    val low24hr: BigDecimal
)

@JsonFormat(shape = JsonFormat.Shape.ARRAY)
data class DayExchangeVolume(
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm")
    val time: LocalDateTime,
    val usersOnline: Int,
    val baseCurrency24HVolume: Map<Currency, BigDecimal>
)


enum class WalletType(@get:JsonValue val id: String) {
    Exchange("e"),
    Margin("m"),
    Lending("l")
}

enum class FundingType(@get:JsonValue val id: Int) {
    ExchangeWallet(0),
    BorrowedFunds(1),
    MarginFunds(2),
    LendingFunds(3);

    companion object {
        @JsonCreator
        @JvmStatic
        fun valueById(id: Int) = values().find { it.id == id }
    }
}

enum class OrderUpdateType(@get:JsonValue val id: String) {
    Filled("f"),
    Cancelled("c"),
    SelfTrade("s"),
}
