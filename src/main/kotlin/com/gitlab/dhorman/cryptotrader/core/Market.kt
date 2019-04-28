package com.gitlab.dhorman.cryptotrader.core

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.Currency
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.CurrencyType
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType

@JsonSerialize(using = Market.Companion.Encoder::class)
@JsonDeserialize(using = Market.Companion.Decoder::class)
data class Market(val baseCurrency: Currency, val quoteCurrency: Currency) {
    val b get() = baseCurrency
    val q get() = quoteCurrency

    fun tpe(currency: Currency): CurrencyType? {
        if (currency == baseCurrency) return CurrencyType.Base
        if (currency == quoteCurrency) return CurrencyType.Quote
        return null
    }

    fun currency(tpe: CurrencyType): Currency {
        return when (tpe) {
            CurrencyType.Base -> baseCurrency
            CurrencyType.Quote -> quoteCurrency
        }
    }

    fun other(currency: Currency): Currency? {
        if (currency == baseCurrency) return quoteCurrency
        if (currency == quoteCurrency) return baseCurrency
        return null
    }

    fun orderType(targetCurrency: Currency): OrderType? {
        val currencyType = tpe(targetCurrency)

        return if (currencyType != null) {
            orderType(currencyType)
        } else {
            null
        }
    }

    fun orderType(targetCurrencyType: CurrencyType): OrderType {
        return when (targetCurrencyType) {
            CurrencyType.Base -> OrderType.Sell
            CurrencyType.Quote -> OrderType.Buy
        }
    }

    fun targetCurrency(orderType: OrderType): Currency {
        return when (orderType) {
            OrderType.Sell -> baseCurrency
            OrderType.Buy -> quoteCurrency
        }
    }

    fun contains(currency: Currency): Boolean {
        return currency == baseCurrency || currency == quoteCurrency
    }

    fun find(currencies: Iterable<Currency>): Currency? {
        return currencies.find { currency -> currency == baseCurrency || currency == quoteCurrency }
    }

    override fun toString(): String = "${baseCurrency}_$quoteCurrency"

    companion object {
        class Encoder : JsonSerializer<Market>() {
            override fun serialize(value: Market, gen: JsonGenerator, serializers: SerializerProvider) {
                gen.writeString(value.toString())
            }
        }

        class Decoder : JsonDeserializer<Market>() {
            override fun deserialize(p: JsonParser, ctx: DeserializationContext): Market {
                if (p.currentToken() == JsonToken.VALUE_STRING) {
                    return p.valueAsString.toMarket()
                } else {
                    throw JsonMappingException(ctx.parser, "Can't parse market")
                }
            }
        }

        class KeyDecoder : KeyDeserializer() {
            override fun deserializeKey(key: String?, ctxt: DeserializationContext?): Any {
                return key?.toMarket() ?: throw Exception("Key can't be null")
            }
        }

        fun String.toMarket(): Market {
            val currencies = this.split('_')

            if (currencies.size == 2) {
                return Market(currencies[0], currencies[1])
            } else {
                throw Exception("""Market "$this" not recognized""")
            }
        }
    }
}
