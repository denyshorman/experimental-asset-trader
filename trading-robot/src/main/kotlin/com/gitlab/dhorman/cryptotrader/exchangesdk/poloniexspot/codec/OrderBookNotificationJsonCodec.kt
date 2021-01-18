package com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.*
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderBookInit
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderBookNotification
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderBookTrade
import com.gitlab.dhorman.cryptotrader.exchangesdk.poloniexspot.model.OrderBookUpdate

object OrderBookNotificationJsonCodec {
    class Decoder : JsonDeserializer<OrderBookNotification>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): OrderBookNotification {
            val arrayNode: ArrayNode = p.readValueAsTree()
            val codec = p.codec as ObjectMapper
            return when (val type = arrayNode[0].textValue()) {
                "i" -> codec.treeToValue<OrderBookInit>(arrayNode[1].path("orderBook"))!!
                "o" -> codec.treeToValue<OrderBookUpdate>(arrayNode)!!
                "t" -> codec.treeToValue<OrderBookTrade>(arrayNode)!!
                else -> throw Exception("Not recognized order book update type $type")
            }
        }
    }
}
