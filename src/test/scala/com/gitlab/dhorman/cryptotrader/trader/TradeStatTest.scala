package com.gitlab.dhorman.cryptotrader.trader

import java.time.Instant

import com.gitlab.dhorman.cryptotrader.core.TradeStatOrder
import com.gitlab.dhorman.cryptotrader.util.BigDecimalUtil._
import com.gitlab.dhorman.cryptotrader.trader.Trader.TradeStatModels.{SimpleTrade, Trade2State}
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.FlatSpec

import scala.util.Random

class TradeStatTest extends FlatSpec {
  val random = new Random()

  private def calcTrueStat(lastTrades: Vector[SimpleTrade]): TradeStatOrder = {

    val waitMsSample = lastTrades.view
      .map(_.timestamp.toEpochMilli)
      .sliding(2)
      .map(t => t.head - t.last)
      .toTraversable

    val waitMsSampleSize = waitMsSample.size

    val avg = BigDecimal(waitMsSample.sum) / waitMsSampleSize

    val variance = {
      val squareSum = waitMsSample.map(v => {
        val d = v - avg
        d * d
      }).sum

      squareSum / waitMsSampleSize
    }

    val stdDev = Math.sqrt(variance.toDouble)

    val sizeAmount = lastTrades.view.map(_.amount).size
    val minAmount = lastTrades.view.map(_.amount).min
    val maxAmount = lastTrades.view.map(_.amount).max
    val avgAmount = {
      val a = lastTrades.view.map(_.amount)
      if (sizeAmount == 0) BigDecimal(0) else a.sum / sizeAmount
    }
    val varianceAmount = lastTrades.view
      .map(_.amount)
      .map(a => {val x = a - avgAmount; x*x})
      .sum / sizeAmount
    val stdDevAmount = Math.sqrt(varianceAmount.toDouble)

    TradeStatOrder(
      avg.toLong,
      variance.toLong,
      stdDev.toLong,
      minAmount,
      maxAmount,
      avgAmount,
      varianceAmount,
      stdDevAmount,
      lastTrades.last.timestamp,
      lastTrades.head.timestamp,
    )
  }

  private def assert(stat: TradeStatOrder, res: Trade2State, checkMinMax: Boolean = true): Unit = {
    assert(stat.ttwAvgMs == res.ttwAverageMs.toLong)
    assert(stat.ttwVariance == res.ttwVariance.toLong)
    assert(stat.ttwStdDev == res.ttwStdDev.toLong)
    if (checkMinMax) assert(stat.minAmount == res.minAmount)
    if (checkMinMax) assert(stat.maxAmount == res.maxAmount)
    assert(stat.avgAmount.cut8 == res.avgAmount.cut8)
    assert(stat.varianceAmount.cut8 == res.varianceAmount.cut8)
    assert(stat.stdDevAmount.cut8 == res.stdDevAmount.cut8)
    assert(stat.firstTranTs == res.firstTranTs)
    assert(stat.lastTranTs == res.lastTranTs)
  }

  "Trade2State calcFull" should "correctly calculate stat with random trades" in {
    val randomTime = random.nextLong().abs % 500 + 1

    val sample = Vector(
      SimpleTrade(3, random.nextLong().abs % 500 + 1, Instant.ofEpochMilli(randomTime)),
      SimpleTrade(2, random.nextLong().abs % 500 + 1, Instant.ofEpochMilli(randomTime - 10)),
      SimpleTrade(1, random.nextLong().abs % 500 + 1, Instant.ofEpochMilli(randomTime - 34)),
    )

    val expected: TradeStatOrder = calcTrueStat(sample)
    val actual: Trade2State = Trade2State.calcFull(sample)

    println(sample)
    println(actual.asJson.spaces2)

    assert(expected, actual)
  }

  "Trade2State calcFull" should "correctly calculate stat with specific trades" in {
    val sample = Vector(
      SimpleTrade(3, 429, Instant.ofEpochMilli(461)),
      SimpleTrade(2, 402, Instant.ofEpochMilli(451)),
      SimpleTrade(1, 308, Instant.ofEpochMilli(427)),
    )

    val expected: TradeStatOrder = calcTrueStat(sample)
    val actual: Trade2State = Trade2State.calcFull(sample)

    println(sample)
    println(actual.asJson.spaces2)

    assert(expected, actual)
  }

  "Trade2State calc" should "correctly calculate stat with random trades" in {
    val randomTime = random.nextLong().abs % 500 + 1

    val sample0 = Vector(
      SimpleTrade(3, random.nextLong().abs % 500 + 1, Instant.ofEpochMilli(randomTime - 5)),
      SimpleTrade(2, random.nextLong().abs % 500 + 1, Instant.ofEpochMilli(randomTime - 8)),
      SimpleTrade(1, random.nextLong().abs % 500 + 1, Instant.ofEpochMilli(randomTime - 15)),
    )
    val sample1 = Vector(
      SimpleTrade(4, 121, Instant.ofEpochMilli(randomTime)),
      sample0(0),
      sample0(1),
      sample0(2),
    )

    val initState: Trade2State = Trade2State.calcFull(sample0)
    val expected: TradeStatOrder = calcTrueStat(sample1)
    val actual = Trade2State.calc(initState, sample0, sample1)

    println(expected.asJson.spaces2)
    println(actual.asJson.spaces2)

    assert(expected, actual)
  }

  "Trade2State calc" should "correctly calculate stat with specific trades" in {
    val sample0 = Vector(
      SimpleTrade(3, 429, Instant.ofEpochMilli(11)),
      SimpleTrade(2, 402, Instant.ofEpochMilli(8)),
      SimpleTrade(1, 308, Instant.ofEpochMilli(1)),
    )
    val sample1 = Vector(
      SimpleTrade(3, 121, Instant.ofEpochMilli(18)),
      sample0(0),
      sample0(1),
      sample0(2),
    )

    val initState: Trade2State = Trade2State.calcFull(sample0)
    val expected: TradeStatOrder = calcTrueStat(sample1)
    val actual = Trade2State.calc(initState, sample0, sample1)

    println(expected.asJson.spaces2)
    println(actual.asJson.spaces2)

    assert(expected, actual)
  }

  "Trade2State calc" should "correctly calculate stat with specific trades 2" in {
    val sample0 = Vector(
      SimpleTrade(3, 3, Instant.ofEpochMilli(11)),
      SimpleTrade(2, 1, Instant.ofEpochMilli(8)),
      SimpleTrade(1, 2, Instant.ofEpochMilli(1)),
    )

    val sample1 = Vector(
      SimpleTrade(4, 4, Instant.ofEpochMilli(18)),
      sample0(0),
      sample0(1),
      sample0(2),
    )

    val sample2 = Vector(
      SimpleTrade(5, 5, Instant.ofEpochMilli(38)),
      sample1(0),
      sample1(1),
      sample1(2),
    )

    val state0 = Trade2State.calcFull(sample0)
    val state1 = Trade2State.calc(state0, sample0, sample1)
    val actual = Trade2State.calc(state1, sample1, sample2)
    val expected = calcTrueStat(sample2)

    println(expected.asJson.spaces2)
    println(actual.asJson.spaces2)

    assert(expected, actual)
  }

  "Trade2State calc" should "correctly calculate stat with specific trades 3" in {
    var ts = 1
    var prevSample: Vector[SimpleTrade] = null
    var sample = Vector[SimpleTrade]()
    var state: Trade2State = Trade2State.calcFull(sample)
    val MaxSample = 15

    for (i <- 1 to 100) {
      val price = random.nextLong().abs % 100 + 1
      val amount = random.nextLong().abs % 500 + 1
      ts += i
      val trade = SimpleTrade(price, amount, Instant.ofEpochMilli(ts))

      prevSample = sample

      if (sample.length == MaxSample) {
        sample = trade +: sample.dropRight(1)
      } else {
        sample = trade +: sample
      }

      state = Trade2State.calc(state, prevSample, sample)
    }

    val expected = calcTrueStat(sample)
    val actual = state

    println(expected.asJson.spaces2)
    println(actual.asJson.spaces2)

    assert(expected, actual, checkMinMax = false)
  }
}
