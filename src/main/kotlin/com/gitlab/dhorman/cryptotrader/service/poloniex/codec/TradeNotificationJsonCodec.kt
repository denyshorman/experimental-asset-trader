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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TradeNotificationJsonCodec {
    private val df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

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
                arrayNode[6].asLong(),
                codec.treeToValue(arrayNode[7]), // TODO: Investigate Can't parse websocket message: arrayNode[7] must not be null
                LocalDateTime.parse(arrayNode[8].asText(), df)
            )
        }
    }
}