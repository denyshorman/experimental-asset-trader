package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderBookTrade

object OrderBookTradeJsonCodec {
    class Decoder : JsonDeserializer<OrderBookTrade>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderBookTrade {
            val arrayNode: ArrayNode = p.readValueAsTree()
            val codec = p.codec as ObjectMapper
            return OrderBookTrade(
                arrayNode[1].asLong(),
                codec.treeToValue(arrayNode[2])!!,
                codec.treeToValue(arrayNode[3])!!,
                codec.treeToValue(arrayNode[4])!!,
                codec.treeToValue(arrayNode[5])!!
            )
        }
    }
}
