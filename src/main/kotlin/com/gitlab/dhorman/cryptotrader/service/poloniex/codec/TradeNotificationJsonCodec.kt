package com.gitlab.dhorman.cryptotrader.service.poloniex.codec

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.gitlab.dhorman.cryptotrader.core.oneMinus
import com.gitlab.dhorman.cryptotrader.service.poloniex.model.TradeNotification
import java.math.BigDecimal

object TradeNotificationJsonCodec {
    class Decoder : JsonDeserializer<TradeNotification>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): TradeNotification {
            val arrayNode: ArrayNode = p.readValueAsTree()
            val codec = p.codec as ObjectMapper
            return TradeNotification(
                arrayNode[1].asLong(),
                codec.treeToValue(arrayNode[2]),
                codec.treeToValue(arrayNode[3]),
                codec.treeToValue<BigDecimal>(arrayNode[4]).oneMinus,
                codec.treeToValue(arrayNode[5]),
                arrayNode[6].asLong()
            )
        }
    }
}