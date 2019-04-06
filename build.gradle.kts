import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "com.gitlab.dhorman"
version = "1.0.0-SNAPSHOT"

val kotlinVersion = "1.3.21"
val kotlinCoroutinesVersion = "1.1.1"
val vertxVersion = "3.7.0"
val reactorAddonsVersion = "3.2.2.RELEASE"
val jacksonVersion = "2.9.8"

plugins {
    kotlin("jvm") version "1.3.21"
    id("kotlinx-serialization") version "1.3.21"
}

repositories {
    mavenCentral()
    jcenter()
    maven(url = "https://kotlin.bintray.com/kotlinx")
}

dependencies {
    implementation("io.vertx:vertx-core:$vertxVersion")
    implementation("io.vertx:vertx-lang-kotlin:$vertxVersion")
    implementation("io.vertx:vertx-web:$vertxVersion")
    implementation("io.vertx:vertx-web-client:$vertxVersion")
    implementation("io.vertx:vertx-reactive-streams:$vertxVersion")
    implementation("io.vertx:vertx-rx-java2:$vertxVersion")
    implementation("io.vertx:vertx-lang-kotlin-coroutines:$vertxVersion")
    implementation("org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion")
    implementation("org.jetbrains.kotlin:kotlin-test-junit5:$kotlinVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-runtime:0.10.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:$kotlinCoroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:$kotlinCoroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-rx2:$kotlinCoroutinesVersion")
    implementation("io.projectreactor:reactor-core:3.2.8.RELEASE")
    implementation("io.projectreactor.addons:reactor-adapter:$reactorAddonsVersion")
    implementation("io.projectreactor.addons:reactor-extra:$reactorAddonsVersion")
    implementation("io.projectreactor.addons:reactor-logback:$reactorAddonsVersion")
    implementation("org.slf4j:slf4j-api:1.7.26")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("io.github.microutils:kotlin-logging:1.6.24")
    implementation("org.kodein.di:kodein-di-erased-jvm:6.1.0")
    implementation("io.vavr:vavr-kotlin:0.10.0")
    implementation("io.vavr:vavr-jackson:0.10.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-parameter-names:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.4.0")
}

tasks.withType<KotlinCompile>().all {
    kotlinOptions.jvmTarget = "1.8"
}
