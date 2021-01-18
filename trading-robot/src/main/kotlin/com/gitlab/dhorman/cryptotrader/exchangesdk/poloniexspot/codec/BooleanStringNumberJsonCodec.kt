package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider

object BooleanStringNumberJsonCodec {
    class Encoder : JsonSerializer<Boolean>() {
        override fun serialize(value: Boolean, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeString(if (value) "1" else "0")
        }
    }

    class Decoder : JsonDeserializer<Boolean>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): Boolean {
            return when(val bool = p.text) {
                "0" -> false
                "1" -> true
                else -> throw Exception("""Not recognized boolean value "$bool"""")
            }
        }
    }
}
