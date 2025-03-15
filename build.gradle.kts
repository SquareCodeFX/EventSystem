plugins {
    id("java")
    id("jacoco") // Code coverage
}

group = "org.example"
version = "1.0-SNAPSHOT"

// Version variables for better dependency management
val slf4jVersion = "2.0.9"
val logbackVersion = "1.4.11"
val guavaVersion = "32.1.2-jre"
val kafkaVersion = "3.5.1"
val rabbitmqVersion = "5.18.0"
val junitVersion = "5.10.0"
val mockitoVersion = "5.6.0"
val awaitilityVersion = "4.2.0"

// Enable build cache for better performance
tasks.withType<JavaCompile> {
    options.isFork = true
    options.isIncremental = true
}

repositories {
    mavenCentral()
}

dependencies {
    // Core dependencies
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("com.google.guava:guava:$guavaVersion")

    // Optional messaging dependencies (marked as compileOnly)
    compileOnly("org.apache.kafka:kafka-clients:$kafkaVersion")
    compileOnly("com.rabbitmq:amqp-client:$rabbitmqVersion")

    // Testing dependencies
    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
    testImplementation("org.awaitility:awaitility:$awaitilityVersion")
}

// Configure JaCoCo for code coverage
jacoco {
    toolVersion = "0.8.11"
}

tasks.jacocoTestReport {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

// Configure test task
tasks.test {
    useJUnitPlatform()

    // Parallel test execution for better performance
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).takeIf { it > 0 } ?: 1

    // Memory settings for test execution
    maxHeapSize = "1g"

    // Generate test reports
    reports {
        html.required.set(true)
        junitXml.required.set(true)
    }

    // Link with JaCoCo
    finalizedBy(tasks.jacocoTestReport)
}

// Configure Java compilation
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17

    // Generate sources and javadoc JARs
    withSourcesJar()
    withJavadocJar()
}
