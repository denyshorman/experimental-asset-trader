package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderKilled

object OrderKilledJsonCodec {
    class Decoder : JsonDeserializer<OrderKilled>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderKilled {
            val arrayNode: ArrayNode = p.readValueAsTree()
            return OrderKilled(
                arrayNode[1].asLong(),
                arrayNode[2].let { if (it.isNull) null else it.asLong() }
            )
        }
    }
}
