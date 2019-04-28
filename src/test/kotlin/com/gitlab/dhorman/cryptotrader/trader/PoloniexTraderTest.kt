package com.gitlab.dhorman.cryptotrader.trader

class PoloniexTraderTest {
    /*@Test
    fun `Test delayed order with put, and trade`() {
        runBlocking {
            val market = Market("USDT", "UAH")
            val marketId = 1
            val amount = Flux.just(BigDecimal("1.2"))

            val minThreshold = BigDecimal("1")
            val threshold = Flux.never<BigDecimal>()

            val accountNotification = EmitterProcessor.create<AccountNotification>()
            val orderBook = ReplayProcessor.cacheLast<OrderBookAbstract>()

            val poloniexApi = object :
                PoloniexApi(Vertx.vertx(), System.getenv("POLONIEX_API_KEY"), System.getenv("POLONIEX_API_SECRET")) {
                override fun buy(
                    market: Market,
                    price: BigDecimal,
                    amount: BigDecimal,
                    tpe: BuyOrderType?
                ): Mono<BuySell> {
                    return Mono.just(BuySell(1, Array.empty())).doOnSuccess {
                        accountNotification.onNext(
                            LimitOrderCreated(
                                marketId,
                                1,
                                OrderType.Buy,
                                BigDecimal("3900.00000001"),
                                BigDecimal("1.2"),
                                LocalDateTime.now()
                            )
                        )

                        orderBook.onNext(
                            PriceAggregatedBook(
                                asks = TreeMap.ofEntries(
                                    tuple(BigDecimal("4100"), BigDecimal("1.2"))
                                ),
                                bids = TreeMap.ofEntries(
                                    compareByDescending { it },
                                    tuple(BigDecimal("3900.00000001"), BigDecimal("1.2")),
                                    tuple(BigDecimal("3900"), BigDecimal("1.2"))
                                )
                            )
                        )

                        accountNotification.onNext(
                            TradeNotification(
                                1,
                                BigDecimal("3900.00000001"),
                                BigDecimal("1.2"),
                                BigDecimal("0.9992"),
                                FundingType.ExchangeWallet,
                                1
                            )
                        )
                    }
                }

                override fun moveOrder(
                    orderId: Long,
                    price: Price,
                    amount: Amount?,
                    orderType: BuyOrderType?
                ): Mono<MoveOrderResult> {
                    return Mono.empty()
                }

                override fun allOpenOrders(): Mono<Map<Long, OpenOrderWithMarket>> {
                    return Mono.just(HashMap.empty<Long, OpenOrderWithMarket>())
                }

                override fun ticker(): Mono<Map<Market, Ticker0>> {
                    return Mono.just(
                        hashMap(
                            market to Ticker0(
                                marketId,
                                BigDecimal.ONE,
                                BigDecimal.ONE,
                                BigDecimal.ONE,
                                BigDecimal.ONE,
                                BigDecimal.ONE,
                                BigDecimal.ONE,
                                false,
                                BigDecimal.ONE,
                                BigDecimal.ONE
                            )
                        )
                    )
                }

                override val tickerStream: Flux<Ticker>
                    get() = Flux.never()

                override val accountNotificationStream: Flux<AccountNotification>
                    get() = accountNotification
            }

            val trader = PoloniexTrader(poloniexApi)

            orderBook.onNext(
                PriceAggregatedBook(
                    asks = TreeMap.ofEntries(
                        tuple(BigDecimal("4100"), BigDecimal("1.2"))
                    ),
                    bids = TreeMap.ofEntries(
                        compareByDescending { it },
                        tuple(BigDecimal("3900"), BigDecimal("1.2"))
                    )
                )
            )

            val trades = trader.buySellDelayed(
                OrderType.Buy,
                market,
                amount,
                orderBook,
                threshold,
                minThreshold
            )

            StepVerifier.create(trades)
                .expectNext(
                    TradeNotification(
                        1,
                        BigDecimal("3900.00000001"),
                        BigDecimal("1.2"),
                        BigDecimal("0.9992"),
                        FundingType.ExchangeWallet,
                        1
                    )
                )
                .expectComplete()
                .verify()
        }
    }

    @Test
    fun `Test delayed order with put, other put, put, trade`() {
        val market = Market("USDT", "UAH")
        val marketId = 1
        val amount = Flux.just(BigDecimal("1.2"))

        val minThreshold = BigDecimal("1")
        val threshold = Flux.never<BigDecimal>()

        val accountNotification = EmitterProcessor.create<AccountNotification>()
        val orderBook = ReplayProcessor.cacheLast<OrderBookAbstract>()

        val poloniexApi = object :
            PoloniexApi(Vertx.vertx(), System.getenv("POLONIEX_API_KEY"), System.getenv("POLONIEX_API_SECRET")) {
            override fun buy(market: Market, price: BigDecimal, amount: BigDecimal, tpe: BuyOrderType?): Mono<BuySell> {
                return Mono.just(BuySell(1, Array.empty())).doOnSuccess {
                    accountNotification.onNext(
                        LimitOrderCreated(
                            marketId,
                            1,
                            OrderType.Buy,
                            BigDecimal("3900.00000001"),
                            BigDecimal("1.2"),
                            LocalDateTime.now()
                        )
                    )

                    orderBook.onNext(
                        PriceAggregatedBook(
                            asks = TreeMap.ofEntries(
                                tuple(BigDecimal("4100"), BigDecimal("1.2"))
                            ),
                            bids = TreeMap.ofEntries(
                                compareByDescending { it },
                                tuple(BigDecimal("3900.00000001"), BigDecimal("1.2")),
                                tuple(BigDecimal("3900"), BigDecimal("1.2"))
                            )
                        )
                    )

                    orderBook.onNext(
                        PriceAggregatedBook(
                            asks = TreeMap.ofEntries(
                                tuple(BigDecimal("4100"), BigDecimal("1.2"))
                            ),
                            bids = TreeMap.ofEntries(
                                compareByDescending { it },
                                tuple(BigDecimal("3900.00000002"), BigDecimal("1.2")),
                                tuple(BigDecimal("3900.00000001"), BigDecimal("1.2"))
                            )
                        )
                    )
                }
            }

            override fun moveOrder(
                orderId: Long,
                price: Price,
                amount: Amount?,
                orderType: BuyOrderType?
            ): Mono<MoveOrderResult2> {
                return Mono.just(MoveOrderResult2(true, 3, HashMap.empty())).doOnSuccess {
                    accountNotification.onNext(OrderUpdate(1, BigDecimal.ZERO))

                    accountNotification.onNext(
                        LimitOrderCreated(
                            marketId,
                            3,
                            OrderType.Buy,
                            BigDecimal("3900.00000003"),
                            BigDecimal("1.2"),
                            LocalDateTime.now()
                        )
                    )

                    accountNotification.onNext(
                        TradeNotification(
                            3,
                            BigDecimal("3900.00000003"),
                            BigDecimal("1.2"),
                            BigDecimal("0.9992"),
                            FundingType.ExchangeWallet,
                            3
                        )
                    )
                }
            }

            override fun allOpenOrders(): Mono<Map<Long, OpenOrderWithMarket>> {
                return Mono.just(HashMap.empty<Long, OpenOrderWithMarket>())
            }

            override fun ticker(): Mono<Map<Market, Ticker0>> {
                return Mono.just(
                    hashMap(
                        market to Ticker0(
                            marketId,
                            BigDecimal.ONE,
                            BigDecimal.ONE,
                            BigDecimal.ONE,
                            BigDecimal.ONE,
                            BigDecimal.ONE,
                            BigDecimal.ONE,
                            false,
                            BigDecimal.ONE,
                            BigDecimal.ONE
                        )
                    )
                )
            }

            override val tickerStream: Flux<Ticker>
                get() = Flux.never()

            override val accountNotificationStream: Flux<AccountNotification>
                get() = accountNotification
        }
        val trader = PoloniexTrader(poloniexApi)

        orderBook.onNext(
            PriceAggregatedBook(
                asks = TreeMap.ofEntries(
                    tuple(BigDecimal("4100"), BigDecimal("1.2"))
                ),
                bids = TreeMap.ofEntries(
                    compareByDescending { it },
                    tuple(BigDecimal("3900"), BigDecimal("1.2"))
                )
            )
        )

        val trades = trader.buySellDelayed(
            OrderType.Buy,
            market,
            amount,
            orderBook,
            threshold,
            minThreshold
        )

        StepVerifier.create(trades)
            .expectNext(
                TradeNotification(
                    3,
                    BigDecimal("3900.00000003"),
                    BigDecimal("1.2"),
                    BigDecimal("0.9992"),
                    FundingType.ExchangeWallet,
                    3
                )
            )
            .expectComplete()
            .verify()
    }*/
}