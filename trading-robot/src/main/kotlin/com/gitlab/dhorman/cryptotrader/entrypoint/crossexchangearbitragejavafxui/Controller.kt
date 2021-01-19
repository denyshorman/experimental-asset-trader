package com.gitlab.dhorman.cryptotrader.entrypoint.crossexchangearbitragejavafxui

import javafx.event.EventHandler
import javafx.fxml.FXML
import javafx.scene.control.Label
import javafx.scene.control.TextField
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.javafx.JavaFx

class Controller {
    private val crossExchangeTrader = createCrossExchangeTrader()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob() + CoroutineName("TraderWindow"))

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            runBlocking {
                scope.coroutineContext[Job]?.cancelAndJoin()
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

        scope.launch {
            // Long-Short coefficients
            crossExchangeTrader.longShortCoeffs.collect { coeffs ->
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

                stateJob = launch {
                    position?.state?.collect {
                        withContext(Dispatchers.JavaFx) {
                            leftPositionStateLabel.text = it.name
                        }
                    }
                }

                profitJob = launch {
                    position?.profit?.collect {
                        withContext(Dispatchers.JavaFx) {
                            leftPositionPnlLabel.text = it.toString()
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
                    position?.state?.collect {
                        withContext(Dispatchers.JavaFx) {
                            rightPositionStateLabel.text = it.name
                        }
                    }
                }

                profitJob = launch {
                    position?.profit?.collect {
                        withContext(Dispatchers.JavaFx) {
                            rightPositionPnlLabel.text = it.toString()
                        }
                    }
                }

                withContext(Dispatchers.JavaFx) {
                    rightPositionSideLabel.text = position?.side?.name ?: "-"
                    rightPositionQuoteAmountLabel.text = position?.quoteAmount?.toString() ?: "-"
                }
            }
        }

        scope.launch {
            crossExchangeTrader.openPositionThreshold.collect { threshold ->
                withContext(Dispatchers.JavaFx) {
                    openPosThresholdTextField.text = threshold.toString()
                }
            }
        }

        scope.launch {
            crossExchangeTrader.closePositionThreshold.collect { threshold ->
                withContext(Dispatchers.JavaFx) {
                    closePosThresholdTextField.text = threshold.toString()
                }
            }
        }
    }
}
