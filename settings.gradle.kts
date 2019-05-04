pluginManagement {
    resolutionStrategy {
        eachPlugin {
            if (requested.id.id == "kotlin") {
                useModule("org.jetbrains.kotlin:kotlin-gradle-plugin:${requested.version}")
            }

            if (requested.id.id == "kotlinx-serialization") {
                useModule("org.jetbrains.kotlin:kotlin-serialization:${requested.version}")
            }

            if (requested.id.id == "org.springframework.boot") {
                useModule("org.springframework.boot:spring-boot-gradle-plugin:${requested.version}")
            }
        }
    }

    repositories {
        gradlePluginPortal()
        maven(url = "https://kotlin.bintray.com/kotlinx")
        maven(url = "https://repo.spring.io/snapshot")
        maven(url = "https://repo.spring.io/milestone")
    }
}

rootProject.name = "crypto-trader"
