package com.gitlab.dhorman.cryptotrader.util.serializer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.math.BigDecimal

@Serializer(BigDecimal::class)
object BigDecimalAsDoubleSerializer : KSerializer<BigDecimal> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(
        serialName = "BigDecimalAsDouble",
        PrimitiveKind.DOUBLE,
    )

    override fun deserialize(decoder: Decoder): BigDecimal {
        return BigDecimal(decoder.decodeDouble().toString())
    }

    override fun serialize(encoder: Encoder, value: BigDecimal) {
        encoder.encodeDouble(value.toDouble())
    }
}
