package com.gitlab.dhorman.cryptotrader.util.serializer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.time.Instant
import java.util.concurrent.TimeUnit

object InstantAsLongNanoSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = Long.serializer().descriptor

    override fun deserialize(decoder: Decoder): Instant {
        return Instant.ofEpochSecond(0L, decoder.decodeLong())
    }

    override fun serialize(encoder: Encoder, value: Instant) {
        encoder.encodeLong(TimeUnit.SECONDS.toNanos(value.epochSecond) + value.nano)
    }
}
