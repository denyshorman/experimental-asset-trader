package com.gitlab.dhorman.cryptotrader.ui.crossexchangefuturestrader

import javafx.application.Application
import javafx.fxml.FXMLLoader
import javafx.scene.Parent
import javafx.scene.Scene
import javafx.stage.Stage

class View : Application() {
    override fun start(stage: Stage) {
        val root = FXMLLoader.load<Parent>(Application::class.java.getResource("/ui/crossexchangefuturestrader/View.fxml"))

        with(stage) {
            title = "Trader"
            scene = Scene(root)
        }

        stage.show()
    }
}
