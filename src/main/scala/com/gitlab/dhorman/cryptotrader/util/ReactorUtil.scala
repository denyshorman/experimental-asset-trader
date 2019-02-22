package com.gitlab.dhorman.cryptotrader.util

import org.reactivestreams.Publisher
import reactor.core.publisher.ReplayProcessor
import reactor.core.scala.publisher.{Flux, FluxProcessor}
import reactor.retry.Retry

import scala.compat.java8.DurationConverters._

object ReactorUtil {
  implicit class FluxExtend(val flux: Flux.type) extends AnyVal {
    def waitAllCompleted(streams: Flux[_]*): Flux[_] = {
      val streams0 = streams.map(_.flatMap(_ => Flux.empty, Flux.error, () => Flux.just(())))
      Flux.zip(streams0, _ => Flux.empty).concatMap(identity)
    }
  }

  implicit def convert(func: Retry[_]): Flux[Throwable] => Publisher[java.lang.Long] = {
    flux: Flux[Throwable] => func.apply(flux.asJava())
  }

  implicit def convert(d: scala.concurrent.duration.FiniteDuration): java.time.Duration = d.toJava
  implicit def convert(d: java.time.Duration): scala.concurrent.duration.FiniteDuration = d.toScala

  def createReplayProcessor[T](defaultValue: Option[T] = None): FluxProcessor[T, T] = {
    val p = if (defaultValue.isDefined) {
      ReplayProcessor.cacheLastOrDefault(defaultValue.get)
    } else {
      ReplayProcessor.cacheLast[T]()
    }

    FluxProcessor.wrap(p, p)
  }
}
