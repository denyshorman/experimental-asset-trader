package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.service.poloniex.TradeVolumeStat
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Amount
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType
import com.gitlab.dhorman.cryptotrader.service.poloniex.volume
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarket
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPartiallyCompleted
import com.gitlab.dhorman.cryptotrader.trader.model.TranIntentMarketPredicted
import io.vavr.Tuple2
import io.vavr.collection.Array
import io.vavr.collection.Map
import io.vavr.collection.Queue
import io.vavr.kotlin.component1
import io.vavr.kotlin.component2
import io.vavr.kotlin.getOrNull
import io.vavr.kotlin.tuple
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.*
import java.util.concurrent.atomic.AtomicLong

class SimulatedPath(
    val orderIntents: Array<OrderIntent>
) {
    val id: Long by lazy(LazyThreadSafetyMode.PUBLICATION) {
        idCounter.incrementAndGet()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as SimulatedPath
        if (id != other.id) return false
        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun toString(): String {
        return "SimulatedPath(id = $id, orderIntents=$orderIntents)"
    }

    data class OrderIntent(
        val market: Market,
        val orderSpeed: OrderSpeed
    )

    companion object {
        private val idCounter = AtomicLong(0)
    }
}

private val ALMOST_ZERO = BigDecimal("0.00000001")


fun SimulatedPath.marketsTinyString(): String {
    return orderIntents.asSequence().map {
        when (it.orderSpeed) {
            OrderSpeed.Instant -> "${it.market}0"
            OrderSpeed.Delayed -> "${it.market}1"
        }
    }.joinToString("->")
}

fun SimulatedPath.targetCurrency(fromCurrency: Currency): Currency? {
    var targetCurrency = fromCurrency
    orderIntents.forEach { orderIntent ->
        targetCurrency = orderIntent.market.other(targetCurrency) ?: return null
    }
    return targetCurrency
}

fun SimulatedPath.OrderIntent.targetAmount(
    fromCurrency: Currency,
    fromCurrencyAmount: Amount,
    fee: FeeMultiplier,
    orderBook: OrderBookAbstract,
    amountCalculator: BuySellAmountCalculator
): Amount {
    val orderTpe = when (fromCurrency) {
        market.baseCurrency -> OrderType.Buy
        market.quoteCurrency -> OrderType.Sell
        else -> throw RuntimeException("Currency $fromCurrency does not exist in market $market")
    }

    when (orderSpeed) {
        OrderSpeed.Instant -> {
            var unusedFromCurrencyAmount: Amount = fromCurrencyAmount
            var targetCurrencyAmount = BigDecimal.ZERO

            when (orderTpe) {
                OrderType.Buy -> {
                    if (orderBook.asks.length() == 0) throw Exception("Can't calculate target amount for instant trade in market $market because order book asks is empty")

                    for ((basePrice, quoteAmount) in orderBook.asks) {
                        val availableFromAmount = amountCalculator.fromAmountBuy(quoteAmount, basePrice, fee.taker)

                        if (unusedFromCurrencyAmount <= availableFromAmount) {
                            val tradeQuoteAmount = amountCalculator.quoteAmount(unusedFromCurrencyAmount, basePrice, fee.taker)
                            targetCurrencyAmount += amountCalculator.targetAmountBuy(tradeQuoteAmount, basePrice, fee.taker)
                            break
                        } else {
                            unusedFromCurrencyAmount -= availableFromAmount
                            targetCurrencyAmount += amountCalculator.targetAmountBuy(quoteAmount, basePrice, fee.taker)
                        }
                    }
                }
                OrderType.Sell -> {
                    if (orderBook.bids.length() == 0) throw Exception("Can't calculate target amount for instant trade in market $market because order book bids is empty")

                    for ((basePrice, quoteAmount) in orderBook.bids) {
                        if (unusedFromCurrencyAmount <= quoteAmount) {
                            targetCurrencyAmount += amountCalculator.targetAmountSell(unusedFromCurrencyAmount, basePrice, fee.taker)
                            break
                        } else {
                            unusedFromCurrencyAmount -= amountCalculator.fromAmountSell(quoteAmount, basePrice, fee.taker)
                            targetCurrencyAmount += amountCalculator.targetAmountSell(quoteAmount, basePrice, fee.taker)
                        }
                    }
                }
            }

            return targetCurrencyAmount
        }
        OrderSpeed.Delayed -> {
            return when (orderTpe) {
                OrderType.Buy -> {
                    if (orderBook.bids.length() == 0) throw Exception("Can't calculate target amount for delayed trade in market $market because order book bids is empty")
                    val price = orderBook.bids.head()._1
                    val quoteAmount = amountCalculator.quoteAmount(fromCurrencyAmount, price, fee.maker)
                    amountCalculator.targetAmountBuy(quoteAmount, price, fee.maker)
                }
                OrderType.Sell -> {
                    if (orderBook.asks.length() == 0) throw Exception("Can't calculate target amount for delayed trade in market $market because order book asks is empty")
                    val price = orderBook.asks.head()._1
                    amountCalculator.targetAmountSell(fromCurrencyAmount, price, fee.maker)
                }
            }
        }
    }
}

fun SimulatedPath.amounts(
    fromCurrency: Currency,
    fromCurrencyAmount: Amount,
    fee: FeeMultiplier,
    orderBooks: Map<Market, out OrderBookAbstract>,
    amountCalculator: BuySellAmountCalculator
): kotlin.Array<Tuple2<Amount, Amount>> {
    var currency = fromCurrency
    var amount = fromCurrencyAmount
    val orderIntentIterator = orderIntents.iterator()

    return Array(orderIntents.size()) {
        val orderIntent = orderIntentIterator.next()
        val orderBook = orderBooks.getOrNull(orderIntent.market) ?: throw Exception("Order book for market ${orderIntent.market} does not exist in map")
        val fromAmount = amount
        amount = orderIntent.targetAmount(currency, fromAmount, fee, orderBook, amountCalculator)
        currency = orderIntent.market.other(currency) ?: throw RuntimeException("Currency $currency does not exist in market ${orderIntent.market}")
        tuple(fromAmount, amount)
    }
}

fun SimulatedPath.targetAmount(
    fromCurrency: Currency,
    fromCurrencyAmount: Amount,
    fee: FeeMultiplier,
    orderBooks: Map<Market, out OrderBookAbstract>,
    amountCalculator: BuySellAmountCalculator
): Amount {
    var currency = fromCurrency
    var amount = fromCurrencyAmount
    for (orderIntent in orderIntents) {
        val orderBook = orderBooks.getOrNull(orderIntent.market) ?: throw Exception("Order book for market ${orderIntent.market} does not exist in map")
        amount = orderIntent.targetAmount(currency, amount, fee, orderBook, amountCalculator)
        currency = orderIntent.market.other(currency) ?: throw RuntimeException("Currency $currency does not exist in market ${orderIntent.market}")
    }
    return amount
}

fun SimulatedPath.OrderIntent.waitTime(
    fromCurrency: Currency,
    fromAmount: Amount,
    tradeVolumeStat: Queue<TradeVolumeStat>
): BigDecimal {
    return when (orderSpeed) {
        OrderSpeed.Instant -> ALMOST_ZERO
        OrderSpeed.Delayed -> {
            var timeSum = ALMOST_ZERO
            val fromCurrencyType = market.tpe(fromCurrency) ?: throw RuntimeException("Currency $fromCurrency does not exist in market $market")
            val orderType = market.orderType(AmountType.From, fromCurrency) ?: throw RuntimeException("Currency $fromCurrency does not exist in market $market")
            tradeVolumeStat.forEach { stat ->
                val volumePerPeriod = stat.volume(fromCurrencyType, orderType)
                val time = when {
                    fromAmount.compareTo(BigDecimal.ZERO) == 0 -> ALMOST_ZERO
                    volumePerPeriod.compareTo(BigDecimal.ZERO) == 0 -> INT_MAX_VALUE_BIG_DECIMAL
                    else -> fromAmount.divide(volumePerPeriod, 16, RoundingMode.HALF_EVEN)
                }
                timeSum += time
            }
            timeSum.divide(tradeVolumeStat.size().toBigDecimal(), 16, RoundingMode.HALF_EVEN)
        }
    }
}

suspend fun SimulatedPath.waitTime(
    fromCurrency: Currency,
    tradeVolumeStatMap: Map<Market, Flow<Queue<TradeVolumeStat>>>,
    amounts: kotlin.Array<Tuple2<Amount, Amount>>
): BigDecimal {
    var currency = fromCurrency
    var waitTimeSum = ALMOST_ZERO
    val amountsIterator = amounts.iterator()
    for (orderIntent in orderIntents) {
        val tradeVolumeStat = tradeVolumeStatMap.getOrNull(orderIntent.market)?.first()
            ?: throw Exception("Volume stat for market ${orderIntent.market} does not exist in map")
        val fromAmount = amountsIterator.next()._1
        waitTimeSum += orderIntent.waitTime(currency, fromAmount, tradeVolumeStat)
        currency = orderIntent.market.other(currency) ?: throw RuntimeException("Currency $currency does not exist in market ${orderIntent.market}")
    }
    return waitTimeSum
}

fun SimulatedPath.toTranIntentMarket(fromAmount: Amount, fromCurrency: Currency): Array<TranIntentMarket> {
    var currency = fromCurrency
    val markets = LinkedList<TranIntentMarket>()
    for ((i, order) in orderIntents.withIndex()) {
        val currencyType = order.market.tpe(currency) ?: return Array.empty()
        currency = order.market.other(currency) ?: return Array.empty()

        val market = if (i == 0) {
            TranIntentMarketPartiallyCompleted(order.market, order.orderSpeed, currencyType, fromAmount)
        } else {
            TranIntentMarketPredicted(order.market, order.orderSpeed, currencyType)
        }

        markets.add(market)
    }
    return Array.ofAll(markets)
}

fun Array<TranIntentMarket>.concat(marketIdx: Int, other: Array<TranIntentMarket>): Array<TranIntentMarket> {
    return this.dropRight(this.length() - marketIdx).appendAll(other)
}
