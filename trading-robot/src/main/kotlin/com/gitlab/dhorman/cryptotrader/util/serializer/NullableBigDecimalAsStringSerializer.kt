package com.gitlab.dhorman.cryptotrader.util.serializer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.math.BigDecimal

object NullableBigDecimalAsStringSerializer : KSerializer<BigDecimal?> {
    override val descriptor: SerialDescriptor = String.serializer().nullable.descriptor

    override fun deserialize(decoder: Decoder): BigDecimal? {
        return if (decoder.decodeNotNullMark()) {
            decoder.decodeString().toBigDecimalOrNull()
        } else {
            decoder.decodeNull()
        }
    }

    override fun serialize(encoder: Encoder, value: BigDecimal?) {
        if (value == null) {
            encoder.encodeNull()
        } else {
            encoder.encodeString(value.toPlainString())
        }
    }
}
