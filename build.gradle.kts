repositories {
    mavenLocal()
    mavenCentral()
}

plugins {
    id("java")
}

dependencies {
    implementation("javax.xml.bind:jaxb-api:2.3.1")
    compileOnly("com.google.code.findbugs:annotations:3.0.1u2")
    testImplementation("io.netty:netty-handler:4.2.12.Final")
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.testcontainers:postgresql:1.21.4")
}

tasks.withType<JavaCompile> {
    options.encoding = "utf-8"
}

group = "com.github.pgasync"
version = "1.0.5"
