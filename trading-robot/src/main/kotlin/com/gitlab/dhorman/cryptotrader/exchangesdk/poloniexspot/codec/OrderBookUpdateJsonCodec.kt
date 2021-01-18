package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderBookUpdate

object OrderBookUpdateJsonCodec {
    class Decoder : JsonDeserializer<OrderBookUpdate>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderBookUpdate {
            val arrayNode: ArrayNode = p.readValueAsTree()
            val codec = p.codec as ObjectMapper
            return OrderBookUpdate(
                codec.treeToValue(arrayNode[1])!!,
                codec.treeToValue(arrayNode[2])!!,
                codec.treeToValue(arrayNode[3])!!
            )
        }
    }
}
