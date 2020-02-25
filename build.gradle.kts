import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "com.gitlab.dhorman"
version = versioning.info.full
java.sourceCompatibility = JavaVersion.VERSION_1_8

val swaggerVersion = "3.0.0-SNAPSHOT"

val developmentOnly by configurations.creating
configurations {
    runtimeClasspath {
        extendsFrom(developmentOnly)
    }
}

plugins {
    kotlin("jvm") version "1.3.61"
    kotlin("plugin.spring") version "1.3.61"
    id("org.springframework.boot") version "2.2.4.RELEASE"
    id("io.spring.dependency-management") version "1.0.9.RELEASE"
    id("net.nemerosa.versioning") version "2.8.2"
}

repositories {
    mavenCentral()
    jcenter()
    maven(url = "https://kotlin.bintray.com/kotlinx")
    maven(url = "https://repo.spring.io/snapshot")
    maven(url = "https://repo.spring.io/milestone")
    maven(url = "http://oss.jfrog.org/artifactory/oss-snapshot-local/") // swagger
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-test-junit5")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-rx2")
    implementation("io.projectreactor.addons:reactor-adapter")
    implementation("io.projectreactor.addons:reactor-extra")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    implementation("org.slf4j:slf4j-api:1.7.26")
    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("io.github.microutils:kotlin-logging:1.6.26")
    implementation("io.vavr:vavr-kotlin:0.10.0")
    implementation("io.vavr:vavr-jackson:0.10.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.module:jackson-module-parameter-names")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-rsocket")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("io.springfox:springfox-swagger2:$swaggerVersion")
    implementation("io.springfox:springfox-swagger-ui:$swaggerVersion")
    implementation("io.springfox:springfox-spring-webflux:$swaggerVersion")
    implementation("io.springfox:springfox-bean-validators:$swaggerVersion")
    implementation("org.springframework.boot.experimental:spring-boot-starter-data-r2dbc")
    implementation("org.springframework.plugin:spring-plugin-core:1.2.0.RELEASE") // TODO: Remove when springfox and spring will be released

    runtimeOnly("io.r2dbc:r2dbc-postgresql")
    developmentOnly("org.springframework.boot:spring-boot-devtools")

    testImplementation("org.junit.jupiter:junit-jupiter:5.4.0")
    testImplementation("org.mockito:mockito-core:2.26.0")
    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.1.0")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.springframework.boot.experimental:spring-boot-test-autoconfigure-r2dbc")
    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
    }
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot.experimental:spring-boot-bom-r2dbc:0.1.0.M3")
    }
}

tasks {
    withType<KotlinCompile>().all {
        with(kotlinOptions) {
            jvmTarget = "1.8"

            freeCompilerArgs = freeCompilerArgs + listOf(
                "-Xjsr305=strict",
                "-Xuse-experimental=kotlinx.coroutines.ExperimentalCoroutinesApi",
                "-Xuse-experimental=kotlinx.coroutines.ObsoleteCoroutinesApi",
                "-Xuse-experimental=kotlinx.coroutines.FlowPreview"
            )
        }
    }

    withType<Test> {
        useJUnitPlatform()
    }
}

springBoot {
    buildInfo {
        properties {
            group = rootProject.group as String
            artifact = rootProject.name
            version = rootProject.version as String
            name = rootProject.name
        }
    }
}
