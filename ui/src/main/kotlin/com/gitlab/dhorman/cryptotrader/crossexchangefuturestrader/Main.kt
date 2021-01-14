package com.gitlab.dhorman.cryptotrader.crossexchangefuturestrader

import javafx.application.Application
import javafx.event.EventHandler
import javafx.scene.Scene
import javafx.scene.control.Button
import javafx.scene.layout.StackPane
import javafx.stage.Stage

class Main : Application() {
    override fun start(stage: Stage) {
        val btn = Button()
        with(btn) {
            text = "Click"
            onMouseClicked = EventHandler {
                println("clicked")
            }
        }

        val root = StackPane()
        with(root) {
            children.add(btn)
        }

        // val root = FXMLLoader.load<Parent>(Application::class.java.getResource("/com/gitlab/dhorman/cryptotrader/view/Test.fxml"))

        with(stage) {
            title = "Trader"
            scene = Scene(root, 300.0, 250.0)
        }

        stage.show()
    }
}

fun main(args: Array<String>) {
    Application.launch(Main::class.java, *args)
}
