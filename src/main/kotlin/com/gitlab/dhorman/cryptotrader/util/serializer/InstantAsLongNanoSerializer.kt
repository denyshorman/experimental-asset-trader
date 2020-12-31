package com.gitlab.dhorman.cryptotrader.util.serializer

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.time.Instant
import java.util.concurrent.TimeUnit

@Serializer(Instant::class)
object InstantAsLongNanoSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = buildSerialDescriptor(
        serialName = "InstantAsLongNano",
        PrimitiveKind.LONG,
    )

    override fun deserialize(decoder: Decoder): Instant {
        return Instant.ofEpochSecond(0L, decoder.decodeLong())
    }

    override fun serialize(encoder: Encoder, value: Instant) {
        TimeUnit.SECONDS.toNanos(value.epochSecond) + value.nano
    }
}
