# syntax=docker/dockerfile:1.7
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app

# 1) Copy Gradle wrapper/build descriptors first to maximize layer cache reuse.
COPY gradlew ./
COPY gradle ./gradle
COPY build.gradle.kts settings.gradle.kts ./

# Warm dependency/cache layers (BuildKit cache mount keeps ~/.gradle across builds).
RUN --mount=type=cache,target=/root/.gradle \
    chmod +x gradlew && ./gradlew --no-daemon dependencies > /dev/null

# 2) Copy sources after dependency resolution so code-only changes don't bust dependency cache.
COPY src ./src

RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

ARG OTEL_AGENT_VERSION=2.9.0
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar /app/otel-agent.jar

COPY --from=builder /app/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["/bin/sh", "-c", "exec java -javaagent:/app/otel-agent.jar -Dotel.resource.attributes=service.instance.id=$(hostname) -jar /app/app.jar"]
