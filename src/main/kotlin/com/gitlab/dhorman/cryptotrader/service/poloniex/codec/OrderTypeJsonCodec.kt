package com.gitlab.dhorman.cryptotrader.service.poloniex.codec

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.*
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderType

object OrderTypeJsonCodec {
    class Encoder : JsonSerializer<OrderType>() {
        override fun serialize(value: OrderType, gen: JsonGenerator, serializers: SerializerProvider) {
            val json = when (value) {
                OrderType.Sell -> "SELL"
                OrderType.Buy -> "BUY"
            }

            gen.writeString(json)
        }
    }

    class Decoder : JsonDeserializer<OrderType>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderType = when (p.currentToken()) {
            JsonToken.VALUE_STRING -> when (val str = p.valueAsString.toUpperCase()) {
                "SELL" -> OrderType.Sell
                "BUY" -> OrderType.Buy
                "0" -> OrderType.Sell
                "1" -> OrderType.Buy
                else -> throw JsonMappingException(ctx.parser, """Not recognized string "$str" """)
            }
            JsonToken.VALUE_NUMBER_INT -> when (val num = p.valueAsInt) {
                0 -> OrderType.Sell
                1 -> OrderType.Buy
                else -> throw JsonMappingException(ctx.parser, """Not recognized integer "$num" """)
            }
            else -> throw JsonMappingException(ctx.parser, "Not recognized OrderType")
        }
    }
}