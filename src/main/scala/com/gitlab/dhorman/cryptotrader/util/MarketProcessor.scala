package com.gitlab.dhorman.cryptotrader.util

object MarketProcessor extends App {
  case class Market(a: String, b: String) {
    override def toString: String = s"${a}_$b"

    override def equals(o: Any): Boolean = o match {
      case m: Market => (m.a == this.a || m.a == this.b) && (m.b == this.a || m.b == this.b)
      case _ => false
    }

    override def hashCode(): Int = a.hashCode + b.hashCode

    def contains(x: String): Boolean = x == a || x == b

    def other(x: String): String = if (x == a) b else a
  }

  val markets = Set(
    Market("BTC", "BCN"),
    Market("BTC", "BTS"),
    Market("BTC", "BURST"),
    Market("BTC", "CLAM"),
    Market("BTC", "DGB"),
    Market("BTC", "DOGE"),
    Market("BTC", "DASH"),
    Market("BTC", "GAME"),
    Market("BTC", "HUC"),
    Market("BTC", "LTC"),
    Market("BTC", "MAID"),
    Market("BTC", "OMNI"),
    Market("BTC", "NAV"),
    Market("BTC", "NMC"),
    Market("BTC", "NXT"),
    Market("BTC", "PPC"),
    Market("BTC", "STR"),
    Market("BTC", "SYS"),
    Market("BTC", "VIA"),
    Market("BTC", "VTC"),
    Market("BTC", "XCP"),
    Market("BTC", "XMR"),
    Market("BTC", "XPM"),
    Market("BTC", "XRP"),
    Market("BTC", "XEM"),
    Market("BTC", "ETH"),
    Market("BTC", "SC"),
    Market("BTC", "FCT"),
    Market("BTC", "DCR"),
    Market("BTC", "LSK"),
    Market("BTC", "LBC"),
    Market("BTC", "STEEM"),
    Market("BTC", "SBD"),
    Market("BTC", "ETC"),
    Market("BTC", "REP"),
    Market("BTC", "ARDR"),
    Market("BTC", "ZEC"),
    Market("BTC", "STRAT"),
    Market("BTC", "PASC"),
    Market("BTC", "GNT"),
    Market("BTC", "BCH"),
    Market("BTC", "ZRX"),
    Market("BTC", "CVC"),
    Market("BTC", "OMG"),
    Market("BTC", "GAS"),
    Market("BTC", "STORJ"),
    Market("BTC", "EOS"),
    Market("BTC", "SNT"),
    Market("BTC", "KNC"),
    Market("BTC", "BAT"),
    Market("BTC", "LOOM"),
    Market("BTC", "QTUM"),
    Market("BTC", "BNT"),
    Market("BTC", "MANA"),
    Market("BTC", "FOAM"),
    Market("BTC", "BCHABC"),
    Market("BTC", "BCHSV"),
    Market("BTC", "NMR"),
    Market("BTC", "POLY"),
    Market("BTC", "LPT"),
    Market("USDT", "BTC"),
    Market("USDT", "DOGE"),
    Market("USDT", "DASH"),
    Market("USDT", "LTC"),
    Market("USDT", "NXT"),
    Market("USDT", "STR"),
    Market("USDT", "XMR"),
    Market("USDT", "XRP"),
    Market("USDT", "ETH"),
    Market("USDT", "SC"),
    Market("USDT", "LSK"),
    Market("USDT", "ETC"),
    Market("USDT", "REP"),
    Market("USDT", "ZEC"),
    Market("USDT", "GNT"),
    Market("USDT", "BCH"),
    Market("USDT", "ZRX"),
    Market("USDT", "EOS"),
    Market("USDT", "SNT"),
    Market("USDT", "KNC"),
    Market("USDT", "BAT"),
    Market("USDT", "LOOM"),
    Market("USDT", "QTUM"),
    Market("USDT", "BNT"),
    Market("USDT", "MANA"),
    Market("XMR", "BCN"),
    Market("XMR", "DASH"),
    Market("XMR", "LTC"),
    Market("XMR", "MAID"),
    Market("XMR", "NXT"),
    Market("XMR", "ZEC"),
    Market("ETH", "LSK"),
    Market("ETH", "STEEM"),
    Market("ETH", "ETC"),
    Market("ETH", "REP"),
    Market("ETH", "ZEC"),
    Market("ETH", "GNT"),
    Market("ETH", "BCH"),
    Market("ETH", "ZRX"),
    Market("ETH", "CVC"),
    Market("ETH", "OMG"),
    Market("ETH", "GAS"),
    Market("ETH", "EOS"),
    Market("ETH", "SNT"),
    Market("ETH", "KNC"),
    Market("ETH", "BAT"),
    Market("ETH", "LOOM"),
    Market("ETH", "QTUM"),
    Market("ETH", "BNT"),
    Market("ETH", "MANA"),
    Market("USDC", "BTC"),
    Market("USDC", "DOGE"),
    Market("USDC", "LTC"),
    Market("USDC", "STR"),
    Market("USDC", "USDT"),
    Market("USDC", "XMR"),
    Market("USDC", "XRP"),
    Market("USDC", "ETH"),
    Market("USDC", "ZEC"),
    Market("USDC", "BCH"),
    Market("USDC", "FOAM"),
    Market("USDC", "BCHABC"),
    Market("USDC", "BCHSV"),
  ).view.map(m => (m, m)).toMap

  type Path = List[Market]

  def f1(targetMarket: Market): Set[Path] = {
    if (markets.contains(targetMarket)) {
      Set(List(targetMarket))
    } else {
      Set(List())
    }
  }

  def f2(targetMarket: Market): Set[Path] = {
    // (a_x - y_b)
    for {
      a <- Set(targetMarket.a)
      b <- Set(targetMarket.b)
      x <- markets.keys.filter(_.contains(a)).map(_.other(a))
      y <- markets.keys.filter(_.contains(b)).map(_.other(b))
      if a != b && a != x && y != b && x == y
    } yield List(Market(a,x), Market(y,b))
  }

  def f3(targetMarket: Market): Set[Path] = {
    // (a_x - x_y - y_b)
    val s = Set(targetMarket.a, targetMarket.b)

    for {
      a <- s
      b <- s
      x <- markets.keys.filter(_.contains(a)).map(_.other(a))
      y <- markets.keys.filter(_.contains(b)).map(_.other(b))
      if !s.contains(x) && !s.contains(y) && x != y && markets.contains(Market(x,y))
    } yield List(Market(a,x), Market(x,y), Market(y,b))
  }

  def f4(targetMarket: Market): Set[Path] = {
    // (a_x - x_i - j_y - y_b)
    val s = Set(targetMarket.a, targetMarket.b)

    for {
      a <- s
      b <- s
      x <- markets.keys.filter(_.contains(a)).map(_.other(a))
      y <- markets.keys.filter(_.contains(b)).map(_.other(b))
      i <- markets.keys.filter(_.contains(x)).map(_.other(x))
      j <- markets.keys.filter(_.contains(y)).map(_.other(y))
      if !s.contains(x) && !s.contains(y) && !s.contains(i) && !s.contains(j) && i == j && x != y
    } yield List(Market(a,x), Market(x,i), Market(j,y), Market(y,b))
  }

  def f(targetMarket: Market): Set[Path] = {
    f1(targetMarket) ++ f2(targetMarket) ++ f3(targetMarket) ++ f4(targetMarket)
  }

  def normalize(paths: Set[Path]): Set[Path] = {
    paths.map(path => path.map(market => markets(market)))
  }

  val targetMarket = Market("USDT", "USDC")
  val marketPaths = f(targetMarket)

  val normalizedPaths = normalize(marketPaths)

  normalizedPaths.toList.sortBy(_.size).foreach(m => {
    println(m)
  })
}
