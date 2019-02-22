package com.gitlab.dhorman.cryptotrader.core

import com.gitlab.dhorman.cryptotrader.core.Orders.{DelayedOrder, InstantDelayedOrder, InstantOrder}
import io.circe._
import io.circe.generic.auto._

class MarketPathGenerator(availableMarkets: Iterable[Market]) {
  import MarketPathGenerator._

  private val allPaths = availableMarkets.view
    .flatMap(m => Set(((m.b, m.q), m), ((m.q, m.b), m)))
    .groupBy(p => p._1._1)
    .map(x => (x._1, x._2.toSet))

  /**
    * Generate paths only for a->b
    */
  def generate(targetPath: TargetPath): Set[Path] = {
    f(targetPath).filter(_.nonEmpty)
  }

  /**
    * Generate all path permutations (a->a) (a->b) (b->a) (b->b)
    */
  def generateAll(targetPath: TargetPath): Map[TargetPath, Set[Path]] = {
    val (a, b) = targetPath
    val p = Array((a, a), (a, b), (b, a), (b, b))

    Map(
      p(0) -> generate(p(0)),
      p(1) -> generate(p(1)),
      p(2) -> generate(p(2)),
      p(3) -> generate(p(3)),
    )
  }

  def generateAllPermutationsWithOrders(currencies: Iterable[Currency]): Map[TargetPath, Set[List[(PathOrderType, Market)]]] = {
    val permutations = for {
      a <- currencies
      b <- currencies
    } yield {
      val target = (a,b)
      val paths = generate(target).flatMap(generateAllPermutationsWithOrders)
      (target, paths)
    }

    permutations.toMap
  }

  private def generateAllPermutationsWithOrders(path: List[Market]): Seq[List[(PathOrderType, Market)]] = {
    (0 until 1 << path.length).map{i =>
      path.zipWithIndex.map{case (market, j) =>
        if ((i & (1 << j)) == 0) {
          (PathOrderType.Delayed, market)
        } else {
          (PathOrderType.Instant, market)
        }
      }
    }
  }

  private def f1(targetPath: TargetPath): Set[Path] = {
    allPaths(targetPath._1)
      .find(_._1._2 == targetPath._2)
      .map(m => Set(List(m._2)))
      .getOrElse({ Set(List()) })
  }

  private def f2(targetPath: TargetPath): Iterable[Path] = {
    // (a->x - y->b)
    val (p, q) = targetPath
    for {
      ((_, x), m1) <- allPaths(p).view.filter(_._1._2 != q)
      (_, m2) <- allPaths(x).view.filter(_._1._2 == q)
    } yield List(m1, m2)
  }

  private def f3(targetPath: TargetPath): Iterable[Path] = {
    // (a->x - y->z - k->b)
    val (p, q) = targetPath
    for {
      ((_, x), m1) <- allPaths(p).view.filter(_._1._2 != q)
      ((_, z), m2) <- allPaths(x).view.filter(h => h._1._2 != p && h._1._2 != q)
      (_, m3) <- allPaths(z).view.filter(_._1._2 == q)
    } yield List(m1, m2, m3)
  }

  private def f4(targetPath: TargetPath): Iterable[Path] = {
    // (a->x - y->z - i->j - k->b)
    val (p, q) = targetPath
    for {
      ((_, x), m1) <- allPaths(p).view.filter(_._1._2 != q)
      ((y, z), m2) <- allPaths(x).view.filter(h => h._1._2 != p && h._1._2 != q)
      ((_, j), m3) <- allPaths(z).view.filter(h => h._1._2 != p && h._1._2 != q && h._1._2 != y)
      (_, m4) <- allPaths(j).view.filter(_._1._2 == q)
    } yield List(m1, m2, m3, m4)
  }

  private def f(targetPath: TargetPath): Set[Path] = {
    f1(targetPath) ++ f2(targetPath) ++ f3(targetPath) ++ f4(targetPath)
  }
}

object MarketPathGenerator {
  type TargetPath = (Currency, Currency)
  type Path = List[Market]

  case class TargetPath0(from: Currency, to: Currency)
  type InstantDelayedOrderChain = List[InstantDelayedOrder]

  case class ExhaustivePath(
    targetPath: TargetPath,
    chain: InstantDelayedOrderChain
  ) {
    @volatile lazy val id: String = chain.view.map {
      case i: InstantOrder => s"${i.market}0"
      case d: DelayedOrder => s"${d.market}1"
    }.mkString("")

    @volatile lazy val simpleMultiplier: BigDecimal = chain.view.map {
      case i: InstantOrder => i.orderMultiplierSimple
      case d: DelayedOrder => d.orderMultiplier
    }.product

    @volatile lazy val amountMultiplier: BigDecimal = chain.view.map {
      case i: InstantOrder => i.orderMultiplierAmount
      case d: DelayedOrder => d.orderMultiplier
    }.product

    @volatile lazy val avgWaitTime: BigDecimal = chain.view.map {
      case _: InstantOrder => 0
      case d: DelayedOrder => d.stat.ttwAvgMs
    }.sum

    @volatile lazy val maxWaitTime: BigDecimal = chain.view.map {
      case _: InstantOrder => 0
      case d: DelayedOrder => d.stat.ttwAvgMs + d.stat.ttwStdDev
    }.sum

    @volatile lazy val recommendedStartAmount: Amount = {
      var recommendedStartAmount: BigDecimal = null
      var targetCurrency = targetPath._2

      for (order <- chain.reverseIterator) {
        order match {
          case d: DelayedOrder =>
            if (d.market.quoteCurrency == targetCurrency) {
              recommendedStartAmount = if (recommendedStartAmount == null) {
                d.stat.avgAmount / d.orderMultiplier
              } else {
                (d.stat.avgAmount min recommendedStartAmount) / d.orderMultiplier
              }
            } else {
              recommendedStartAmount = if (recommendedStartAmount == null) {
                d.stat.avgAmount
              } else {
                (recommendedStartAmount * d.orderMultiplier) min d.stat.avgAmount
              }
            }
          case _ => // Ignore
        }

        targetCurrency = order.market.other(targetCurrency).get
      }

      if (recommendedStartAmount == null) {
        recommendedStartAmount = chain.head.fromAmount
      }

      recommendedStartAmount
    }

    override def hashCode(): Int = id.hashCode()

    override def equals(obj: Any): Boolean = obj match {
      case o: ExhaustivePath => id == o.id
      case _ => false
    }
  }

  object ExhaustivePath {
    implicit val encoder: Encoder[ExhaustivePath] = Encoder.forProduct8("targetPath", "chain", "id", "simpleMultiplier", "amountMultiplier", "avgWaitTime", "maxWaitTime", "recommendedStartAmount")(u => {
      (u.targetPath, u.chain, u.id, u.simpleMultiplier, u.amountMultiplier, u.avgWaitTime, u.maxWaitTime, u.recommendedStartAmount)
    })
  }

  type PathOrderType = PathOrderType.Value
  object PathOrderType extends Enumeration {
    val Instant: PathOrderType = Value
    val Delayed: PathOrderType = Value
  }

}
