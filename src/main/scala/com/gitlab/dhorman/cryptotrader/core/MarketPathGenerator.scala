package com.gitlab.dhorman.cryptotrader.core

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
}
