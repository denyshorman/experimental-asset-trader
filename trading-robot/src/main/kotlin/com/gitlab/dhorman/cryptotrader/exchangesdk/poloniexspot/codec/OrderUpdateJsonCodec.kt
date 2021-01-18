package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderUpdate

object OrderUpdateJsonCodec {
    class Decoder : JsonDeserializer<OrderUpdate>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderUpdate {
            val arrayNode: ArrayNode = p.readValueAsTree()
            val codec = p.codec as ObjectMapper
            return OrderUpdate(
                arrayNode[1].asLong(),
                codec.treeToValue(arrayNode[2])!!,
                codec.treeToValue(arrayNode[3])!!,
                arrayNode[4].let { if (it.isNull) null else it.asLong() },
                codec.treeToValue(arrayNode[5])!!
            )
        }
    }
}
