package com.gitlab.dhorman.cryptotrader.entrypoint.crossexchangearbitragejavafxui

import com.gitlab.dhorman.cryptotrader.robots.crossexchangearbitrage.trader.algo.StrategyStat
import javafx.event.EventHandler
import javafx.fxml.FXML
import javafx.scene.control.Label
import javafx.scene.control.TextField
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.javafx.JavaFx

class Controller {
    private val crossExchangeTrader = createCrossExchangeTrader()
    private val strategyStat = StrategyStat(crossExchangeTrader)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("TraderWindow"))

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            runBlocking {
                scope.coroutineContext[Job]?.cancelAndJoin()
                strategyStat.close()
            }
        })
    }

    @FXML
    private lateinit var longShortCoefLabel: Label

    @FXML
    private lateinit var shortLongCoefLabel: Label

    @FXML
    private lateinit var openQuoteAmountLabel: Label

    @FXML
    private lateinit var leftPositionStateLabel: Label

    @FXML
    private lateinit var rightPositionStateLabel: Label

    @FXML
    private lateinit var leftPositionSideLabel: Label

    @FXML
    private lateinit var rightPositionSideLabel: Label

    @FXML
    private lateinit var leftPositionQuoteAmountLabel: Label

    @FXML
    private lateinit var rightPositionQuoteAmountLabel: Label

    @FXML
    private lateinit var leftPositionPnlLabel: Label

    @FXML
    private lateinit var rightPositionPnlLabel: Label

    @FXML
    private lateinit var openPosThresholdTextField: TextField

    @FXML
    private lateinit var closePosThresholdTextField: TextField

    @FXML
    private lateinit var traderStateLabel: Label

    @FXML
    private lateinit var traderCurrentProfitLabel: Label

    @FXML
    private lateinit var traderOnCloseProfitLabel: Label

    @FXML
    private lateinit var longShortMinLabel: Label

    @FXML
    private lateinit var longShortMaxLabel: Label

    @FXML
    private lateinit var shortLongMinLabel: Label

    @FXML
    private lateinit var shortLongMaxLabel: Label

    @FXML
    private fun initialize() {
        openPosThresholdTextField.onMouseExited = EventHandler {
            val threshold = openPosThresholdTextField.text.toBigDecimalOrNull()
            if (threshold != null) {
                crossExchangeTrader.openPositionThreshold.value = threshold
            }
        }

        closePosThresholdTextField.onMouseExited = EventHandler {
            val threshold = closePosThresholdTextField.text.toBigDecimalOrNull()
            if (threshold != null) {
                crossExchangeTrader.closePositionThreshold.value = threshold
            }
        }

        // Long-Short coefficients
        scope.launch {
            crossExchangeTrader.openStrategy.collect { coeffs ->
                withContext(Dispatchers.JavaFx) {
                    longShortCoefLabel.text = coeffs.k0.toString()
                    shortLongCoefLabel.text = coeffs.k1.toString()
                    openQuoteAmountLabel.text = coeffs.quoteAmount.toString()
                }
            }
        }

        // left position
        scope.launch {
            var stateJob: Job? = null
            var profitJob: Job? = null

            crossExchangeTrader.leftMarketPosition.collect { position ->
                stateJob?.cancelAndJoin()
                profitJob?.cancelAndJoin()

                stateJob = launch(start = CoroutineStart.UNDISPATCHED) {
                    if (position == null) {
                        withContext(Dispatchers.JavaFx) {
                            leftPositionStateLabel.text = "-"
                        }
                    } else {
                        position.state.collect {
                            withContext(Dispatchers.JavaFx) {
                                leftPositionStateLabel.text = it.name
                            }
                        }
                    }
                }

                profitJob = launch(start = CoroutineStart.UNDISPATCHED) {
                    if (position == null) {
                        withContext(Dispatchers.JavaFx) {
                            leftPositionPnlLabel.text = "-"
                        }
                    } else {
                        position.profit.collect {
                            withContext(Dispatchers.JavaFx) {
                                leftPositionPnlLabel.text = it.toString()
                            }
                        }
                    }
                }

                withContext(Dispatchers.JavaFx) {
                    leftPositionSideLabel.text = position?.side?.name ?: "-"
                    leftPositionQuoteAmountLabel.text = position?.quoteAmount?.toString() ?: "-"
                }
            }
        }

        // right position
        scope.launch {
            var stateJob: Job? = null
            var profitJob: Job? = null

            crossExchangeTrader.rightMarketPosition.collect { position ->
                stateJob?.cancelAndJoin()
                profitJob?.cancelAndJoin()

                stateJob = launch {
                    if (position == null) {
                        withContext(Dispatchers.JavaFx) {
                            rightPositionStateLabel.text = "-"
                        }
                    } else {
                        position.state.collect {
                            withContext(Dispatchers.JavaFx) {
                                rightPositionStateLabel.text = it.name
                            }
                        }
                    }
                }

                profitJob = launch {
                    if (position == null) {
                        withContext(Dispatchers.JavaFx) {
                            rightPositionPnlLabel.text = "-"
                        }
                    } else {
                        position.profit.collect {
                            withContext(Dispatchers.JavaFx) {
                                rightPositionPnlLabel.text = it.toString()
                            }
                        }
                    }
                }

                withContext(Dispatchers.JavaFx) {
                    rightPositionSideLabel.text = position?.side?.name ?: "-"
                    rightPositionQuoteAmountLabel.text = position?.quoteAmount?.toString() ?: "-"
                }
            }
        }

        // openPositionThreshold
        scope.launch {
            crossExchangeTrader.openPositionThreshold.collect { threshold ->
                withContext(Dispatchers.JavaFx) {
                    openPosThresholdTextField.text = threshold.toString()
                }
            }
        }

        // closePositionThreshold
        scope.launch {
            crossExchangeTrader.closePositionThreshold.collect { threshold ->
                withContext(Dispatchers.JavaFx) {
                    closePosThresholdTextField.text = threshold.toString()
                }
            }
        }

        // trader's state
        scope.launch {
            crossExchangeTrader.state.collect { state ->
                withContext(Dispatchers.JavaFx) {
                    traderStateLabel.text = state.toString()
                }
            }
        }

        // trader's current profit
        scope.launch {
            crossExchangeTrader.currentProfit.collect { profit ->
                withContext(Dispatchers.JavaFx) {
                    traderCurrentProfitLabel.text = profit.toString()
                }
            }
        }

        // trader's on close profit
        scope.launch {
            crossExchangeTrader.onCloseProfit.collect { profit ->
                withContext(Dispatchers.JavaFx) {
                    traderOnCloseProfitLabel.text = profit.toString()
                }
            }
        }

        // strategy stat
        scope.launch {
            strategyStat.openStrategyStat.collect { stat ->
                withContext(Dispatchers.JavaFx) {
                    longShortMinLabel.text = stat.k0Min.toString()
                    longShortMaxLabel.text = stat.k0Max.toString()
                    shortLongMinLabel.text = stat.k1Min.toString()
                    shortLongMaxLabel.text = stat.k1Max.toString()
                }
            }
        }


        // Start trading
        scope.launch {
            crossExchangeTrader.trade()
        }
    }
}
