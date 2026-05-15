FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app
COPY . .
RUN ./gradlew bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

ARG OTEL_AGENT_VERSION=2.9.0
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar /app/otel-agent.jar

COPY --from=builder /app/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["/bin/sh", "-c", "exec java -javaagent:/app/otel-agent.jar -Dotel.resource.attributes=service.instance.id=$(hostname) -jar /app/app.jar"]
