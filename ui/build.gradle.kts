import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "com.gitlab.dhorman"
version = "1.0.0"
java.sourceCompatibility = JavaVersion.VERSION_15

plugins {
    application
    kotlin("jvm") version "1.4.21"
    id("org.openjfx.javafxplugin") version "0.0.9"
}

application {
    mainClass.set("com.gitlab.dhorman.cryptotrader.MainKt")
}

repositories {
    mavenCentral()
    jcenter()
}

dependencies {
    implementation(project(":trading-robot"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-javafx:1.4.2")
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.1")
}

javafx {
    version = "15.0.1"
    modules = listOf("javafx.base", "javafx.controls", "javafx.fxml", "javafx.graphics")
}

tasks {
    withType<KotlinCompile>().all {
        with(kotlinOptions) {
            jvmTarget = "15"

            freeCompilerArgs = freeCompilerArgs + listOf(
                "-Xjsr305=strict"
            )
        }
    }

    withType<Test> {
        useJUnitPlatform()
    }
}
