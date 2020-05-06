package com.gitlab.dhorman.cryptotrader.service.poloniex.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.OrderPendingAck

object OrderPendingAckJsonCodec {
    class Decoder : JsonDeserializer<OrderPendingAck>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderPendingAck {
            val arrayNode: ArrayNode = p.readValueAsTree()
            val codec = p.codec as ObjectMapper
            return OrderPendingAck(
                arrayNode[1].asLong(),
                arrayNode[2].asInt(),
                codec.treeToValue(arrayNode[3])!!,
                codec.treeToValue(arrayNode[4])!!,
                codec.treeToValue(arrayNode[5])!!,
                arrayNode[6].let { if (it.isNull) null else it.asLong() }
            )
        }
    }
}
