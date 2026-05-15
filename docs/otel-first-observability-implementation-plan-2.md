# Production-Grade OpenTelemetry Implementation Plan

## Context

This plan covers distributed tracing for `retail-live-pricing` as Step 4 of the observability roadmap. Steps 1–3 (metrics, alerting, structured logging + correlation IDs) are already in place. The goal is end-to-end trace visibility across the full Kafka pipeline:

```
ingestion → price-ticks → price-updates → portfolio-calc-tasks → portfolio-updates → WS gateway
```

---

## Why Not the Earlier Draft

The first draft (Micrometer SDK + app → Tempo direct) had these production-breaking problems:

| Gap | Impact |
|---|---|
| App sends directly to Tempo | If Tempo restarts, in-flight spans are lost. No buffering, no retry. |
| 100% sampling | Unbounded storage. At 200ms tick interval × 500 symbols × 12 partitions this blows Tempo's block storage in hours. |
| No JDBC/Redis spans | `PortfolioCalculatorService.calculateAndPublish()` does 2 JPA queries + N Redis reads + 1 JPA write. All invisible. The trace is blind inside the heaviest method. |
| `traceId`/`spanId` absent from logs | `logback-spring.xml` emits `correlationId` but not OTel trace IDs. Grafana trace→log deep links can't find log lines for a trace. |
| Broken async span parent chain | `CompletableFuture.runAsync(…, pricingExecutor)` detaches the span context. 500 child spans appear as unparented root traces. |
| High-cardinality `userId` in span attributes | Millions of unique userId values cause Tempo index explosion. |
| WebSocket delivery not instrumented | The final push to the client is invisible in any trace. |
| Dockerfile has no `-javaagent` | Nothing would actually run. |

---

## Instrumentation Strategy: OTel Java Agent

Production companies use the **OpenTelemetry Java Agent** (`opentelemetry-javaagent.jar`) attached via `-javaagent`, not SDK library dependencies, for these reasons:

- Auto-instruments Spring MVC, Spring Kafka, JDBC/Hibernate, Redis (Lettuce), SLF4J MDC — **zero code changes**
- Agent version is independent of app deploys — can patch or upgrade the agent without a full release
- Adding SDK `implementation()` dependencies alongside the agent causes **double-instrumentation** on Kafka and HTTP spans

Configuration is primarily via environment variables (`OTEL_*`). The only `build.gradle.kts` change is one `compileOnly` dependency needed for the WS manual span (Phase 8) — it is not bundled in the fat jar and does not conflict with the agent.

---

## Architecture

```
┌──────────────────────────────────────────┐
│  App (OTel Java Agent attached)          │
│  Instruments: HTTP, Kafka, JDBC, Redis   │
│  MDC: trace_id + span_id injected by agent │
└────────────────┬─────────────────────────┘
                 │ OTLP gRPC  :4317
                 ▼
┌──────────────────────────────────────────┐
│  Grafana Alloy (existing, extended)      │
│  + OTLP receiver                         │
│  + Attribute scrubber (PII/cardinality)  │
│  + Tail-based sampler                    │
│  + Tempo exporter                        │
│  Existing: Docker log → Loki             │
└──────┬───────────────────────────────────┘
       │ OTLP gRPC
       ▼
┌──────────────────────┐     ┌─────────────────────────┐
│  Grafana Tempo       │────▶│  Prometheus (remote_write│
│  Trace storage       │     │  for RED metrics from    │
│  Service graph       │     │  metrics_generator)      │
│  Metrics generator   │     └─────────────────────────┘
└──────────────────────┘
       │ Datasource
       ▼
┌──────────────────────┐
│  Grafana             │
│  Tempo datasource    │
│  trace→log (Loki)    │
│  trace→metrics       │
│  service map         │
│  exemplar links      │
└──────────────────────┘
```

---

## Implementation Phases

### Phase 1: OTel Java Agent Setup

**Files changed: `Dockerfile`, `docker-compose.yml`**

#### `Dockerfile`

```dockerfile
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /app
COPY . .
RUN ./gradlew bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Pin agent version — update in lock-step with OTel SDK minor releases
ARG OTEL_AGENT_VERSION=2.9.0
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar /app/otel-agent.jar

COPY --from=builder /app/build/libs/*.jar app.jar
EXPOSE 8080

ENTRYPOINT ["/bin/sh", "-c", \
  "export OTEL_RESOURCE_ATTRIBUTES=\"deployment.environment=${DEPLOYMENT_ENV:-local},service.version=${SERVICE_VERSION:-0.1.0},service.instance.id=$(hostname)\" && \
   exec java -javaagent:/app/otel-agent.jar -jar /app/app.jar"]
```

#### `docker-compose.yml` — app service environment

```yaml
  app:
    environment:
      OTEL_SERVICE_NAME: retail-live-pricing
      DEPLOYMENT_ENV: local          # consumed by entrypoint to build OTEL_RESOURCE_ATTRIBUTES
      SERVICE_VERSION: "0.1.0"       # consumed by entrypoint to build OTEL_RESOURCE_ATTRIBUTES
      OTEL_EXPORTER_OTLP_ENDPOINT: http://alloy:4317
      OTEL_EXPORTER_OTLP_PROTOCOL: grpc
      OTEL_PROPAGATORS: tracecontext,baggage
      OTEL_INSTRUMENTATION_KAFKA_ENABLED: "true"
      OTEL_INSTRUMENTATION_JDBC_ENABLED: "true"
      OTEL_INSTRUMENTATION_LETTUCE_ENABLED: "true"
      OTEL_LOGS_EXPORTER: none       # logs already go via Alloy → Loki
      OTEL_METRICS_EXPORTER: none    # metrics already go via Micrometer → Prometheus
      OTEL_TRACES_SAMPLER: always_on # all sampling decisions delegated to Alloy tail sampler
```

`OTEL_LOGS_EXPORTER: none` and `OTEL_METRICS_EXPORTER: none` are critical — without them, the agent will ship duplicate metrics into Tempo and duplicate logs to a second backend, conflicting with the existing Micrometer and Logstash pipelines.

`OTEL_TRACES_SAMPLER: always_on` + Alloy tail sampler is the correct pattern. Do **not** use `parentbased_traceidratio` alongside tail sampling — any trace head-dropped at the app never reaches Alloy, breaking the "always keep errors/slow" guarantee. With `always_on`, Alloy is the single authority on what Tempo stores.

`OTEL_RESOURCE_ATTRIBUTES` is built entirely inside the entrypoint — not set in docker-compose. The entrypoint exports one single string combining `DEPLOYMENT_ENV`, `SERVICE_VERSION` (both passed as simple env vars from docker-compose), and `$(hostname)` (shell-evaluated at container start). This eliminates any SDK merge-precedence ambiguity: there is exactly one source of resource attributes, and it is always fully composed before the JVM starts. Do **not** set `OTEL_RESOURCE_ATTRIBUTES` in docker-compose alongside this entrypoint — the entrypoint will overwrite it, but having two sources creates confusion about which value is active. In K8s, replace the entrypoint `$(hostname)` with an env var injected from the downward API (`fieldRef: metadata.name`) and keep the same entrypoint pattern of composing one string.

For local `bootRun` (no docker), export in shell or `.env`:
```bash
export OTEL_SERVICE_NAME=retail-live-pricing
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_TRACES_SAMPLER=always_on
export OTEL_LOGS_EXPORTER=none
export OTEL_METRICS_EXPORTER=none
```

---

### Phase 2: Grafana Alloy — OTLP Trace Pipeline

**Files changed: `ops/alloy/config.alloy`, `docker-compose.yml`**

#### `ops/alloy/config.alloy` — append after existing Loki block

```alloy
// ── OTLP trace ingestion ──────────────────────────────────────────────

otelcol.receiver.otlp "traces" {
  grpc { endpoint = "0.0.0.0:4317" }
  http { endpoint = "0.0.0.0:4318" }
  output {
    traces = [otelcol.processor.attributes.scrub.input]
  }
}

// Strip high-cardinality Kafka message keys (contain userId) before indexing
otelcol.processor.attributes "scrub" {
  action {
    key    = "messaging.kafka.message.key"
    action = "delete"
  }
  output {
    traces = [otelcol.processor.tail_sampling.default.input]
  }
}

otelcol.processor.tail_sampling "default" {
  decision_wait = "10s"

  // 1. Always keep error traces — most valuable signal
  policy {
    name = "always-errors"
    type = "status_code"
    status_code { status_codes = ["ERROR"] }
  }

  // 2. Always keep slow traces (latency > 500ms SLO threshold)
  policy {
    name = "always-slow"
    type = "latency"
    latency { threshold_ms = 500 }
  }

  // 3. Drop noisy health-check root spans
  policy {
    name              = "drop-health-checks"
    type              = "string_attribute"
    string_attribute {
      key                         = "http.target"
      values                      = ["/actuator/health", "/actuator/prometheus"]
      enabled_for_root_spans_only = true
    }
  }

  // 4. Keep 10% of everything else
  policy {
    name            = "probabilistic-baseline"
    type            = "probabilistic"
    probabilistic { sampling_percentage = 10 }
  }

  output {
    traces = [otelcol.processor.batch.default.input]
  }
}

// Coalesce spans into efficient payloads before export — reduces gRPC call overhead under load
otelcol.processor.batch "default" {
  timeout              = "5s"
  send_batch_size      = 512
  send_batch_max_size  = 1024
  output {
    traces = [otelcol.exporter.otlp.tempo.input]
  }
}

otelcol.exporter.otlp "tempo" {
  client {
    endpoint = "tempo:4317"
    tls { insecure = true }
  }
  sending_queue {
    enabled    = true
    queue_size = 10000     // when full, NEW spans are DROPPED (oldest in queue are preserved)
                           // monitor otelcol_exporter_queue_size via Alloy self-metrics at :12345/metrics
                           // alert when queue_size > 8000 (80%) — add rule to prometheus-alerts.yml
  }
  retry_on_failure {
    enabled          = true
    initial_interval = "5s"
    max_interval     = "30s"
    max_elapsed_time = "300s"  // spans are dropped after 5 min of failed retries; this is acceptable
                                // for traces but must be documented in the runbook
  }
}
```

#### `docker-compose.yml` — expose Alloy OTLP ports

```yaml
  alloy:
    ports:
      - "12345:12345"
      - "4317:4317"    # OTLP gRPC — app connects here
      - "4318:4318"    # OTLP HTTP
```

---

### Phase 3: Grafana Tempo Backend

**Files changed: `ops/tempo-config.yml` (new), `docker-compose.yml`**

#### `ops/tempo-config.yml` (new file)

```yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317

ingester:
  max_block_duration: 5m

storage:
  trace:
    backend: local
    local:
      path: /var/tempo/traces
    wal:
      path: /var/tempo/wal

compactor:
  compaction:
    block_retention: 72h    # 3 days locally; set to 7d+ in staging/prod

# Derives RED metrics per service pair and writes to Prometheus.
# This powers the Grafana service dependency graph without any manual instrumentation.
metrics_generator:
  registry:
    external_labels:
      source: tempo
      application: retail-live-pricing
  storage:
    path: /var/tempo/generator/wal
    remote_write:
      - url: http://prometheus:9090/api/v1/write
        send_exemplars: true
  processor:
    service_graphs:
      dimensions: [service.name]
    span_metrics:
      dimensions: [service.name, span.kind]

overrides:
  defaults:
    metrics_generator:
      processors: [service-graphs, span-metrics]
```

#### `docker-compose.yml` — add Tempo + enable Prometheus remote write

```yaml
  tempo:
    image: grafana/tempo:2.6.0
    container_name: live-pricing-tempo
    command: [ "-config.file=/etc/tempo/tempo.yml" ]
    ports:
      - "3200:3200"
    volumes:
      - ./ops/tempo-config.yml:/etc/tempo/tempo.yml:ro
      - tempo-data:/var/tempo
    restart: unless-stopped

  prometheus:
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--enable-feature=exemplar-storage"
      - "--web.enable-remote-write-receiver"    # required for Tempo metrics_generator

  grafana:
    depends_on:
      - prometheus
      - loki
      - tempo                                   # add tempo dependency
```

Add `tempo-data:` to the `volumes:` block.

---

### Phase 4: Trace ↔ Log Correlation

**Files changed: `src/main/resources/logback-spring.xml`**

The OTel Java agent injects `trace_id` and `span_id` (underscore-separated) into SLF4J MDC. The JSON pattern in `logback-spring.xml` must read those exact MDC keys — currently it does not emit them.

Add two fields to the existing `<pattern>` block alongside `correlationId`:

```xml
"traceId":"%mdc{trace_id:-}",
"spanId":"%mdc{span_id:-}"
```

The MDC key names (`trace_id`, `span_id`) must match exactly what the agent writes. The JSON output field names (`traceId`, `spanId`) are chosen to match the Loki query in Phase 5 — they can differ from the MDC keys. Using `%mdc{traceId:-}` (camelCase key) silently returns empty strings because the agent never writes to that key.

These coexist with the existing `correlationId` field:
- `correlationId` — business-level, propagated via `X-Correlation-Id` Kafka header
- `traceId` — OTel W3C trace, propagated via `traceparent` header

The Grafana "Logs for this span" button queries Loki using `traceId` as a JSON field. Without this change, that button returns no results.

---

### Phase 5: Grafana Tempo Datasource + Service Map

**Files changed: `ops/grafana/provisioning/datasources/prometheus.yml`**

Add after the existing Loki entry:

```yaml
  - name: Tempo
    uid: tempo
    type: tempo
    access: proxy
    url: http://tempo:3200
    jsonData:
      tracesToLogsV2:
        datasourceUid: loki
        filterByTraceID: true
        customQuery: true
        query: '{container="live-pricing-app"} | json | traceId=`${__trace.traceId}`'
      tracesToMetrics:
        datasourceUid: prometheus
        spanStartTimeShift: "-1m"
        spanEndTimeShift: "1m"
        queries:
          - name: Request rate
            query: 'rate(http_server_requests_seconds_count{application="retail-live-pricing"}[$__rate_interval])'
      serviceMap:
        datasourceUid: prometheus    # Tempo metrics_generator writes service-graph metrics here
      lokiSearch:
        datasourceUid: loki
      nodeGraph:
        enabled: true               # enables the visual service dependency graph in Grafana
```

---

### Phase 6: Exemplar Links on Dashboard

**Files changed: `ops/grafana/dashboards/latency-and-slo.json`**

On the p95 HTTP latency histogram panel, set `"exemplar": true` on the Prometheus target query.

When Prometheus scrapes histograms with `--enable-feature=exemplar-storage` active, each histogram bucket observation can carry a `traceId` exemplar (the OTel agent does this automatically). Grafana renders them as dots on the histogram line. Clicking a dot jumps to the corresponding trace in Tempo.

---

### Phase 7: Async Context Propagation

**Files changed: `src/main/java/com/retail/livepricing/common/config/AsyncConfig.java`**

`PortfolioTaskConsumer.onBatchTask()` submits work via `CompletableFuture.runAsync(…, pricingExecutor)`. The OTel Java agent instruments `ThreadPoolExecutor` for context propagation, but Spring's `ThreadPoolTaskExecutor` wraps it in a way that may not be picked up.

**Verification first**: after Phase 1 is running, open a trace and check whether the 500 `calculateAndPublish` spans appear as children of the batch consumer span, or as separate root traces. If they're orphans, apply this fix in `AsyncConfig.java`:

```java
import io.opentelemetry.context.Context;

@Bean("pricingExecutor")
public Executor pricingExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(8);
    executor.setMaxPoolSize(32);
    executor.setQueueCapacity(10_000);
    executor.setThreadNamePrefix("pricing-");
    executor.initialize();
    // Wrap so OTel context (active span) propagates into submitted tasks
    return Context.taskWrapping(executor.getThreadPoolExecutor());
}
```

`Context.taskWrapping()` is in `opentelemetry-api`, which is pulled in transitively by the agent's bootstrap classloader. If the compiler can't resolve it, add explicitly:

```kotlin
// build.gradle.kts — compileOnly so it's not bundled (agent provides it at runtime)
compileOnly("io.opentelemetry:opentelemetry-api")
```

---

### Phase 8: WebSocket Delivery Span (E2E Completion)

**Files changed: `src/main/java/com/retail/livepricing/streaming/service/GatewayOutboundService.java`, `build.gradle.kts`**

The OTel agent does not auto-instrument Spring WebSocket. Without this phase, the trace chain ends at `Kafka consume portfolio-updates` and the final delivery hop — the push to the client — is invisible. This phase completes the stated E2E goal.

#### `build.gradle.kts`

```kotlin
// compileOnly — agent provides opentelemetry-api at runtime; do not bundle it in the fat jar
compileOnly("io.opentelemetry:opentelemetry-api")
```

#### `GatewayOutboundService.java`

Prefer **lazy acquisition** — call `GlobalOpenTelemetry.getTracer()` at the point of use rather than in a `static final` field. Although the agent typically initialises before Spring beans, the lazy pattern eliminates any ordering ambiguity and is safe to call repeatedly (the result is cached internally by the SDK):

```java
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
```

Acquire the tracer lazily inside the send method:

```java
Tracer tracer = GlobalOpenTelemetry.getTracer("retail-live-pricing");
```

Wrap the outbound WS send in the delivery method:

```java
// screenContext: resolved from ScreenContext enum before the push decision is made.
// It is already available at this point in the call path (GatewayOutboundService reads
// session state before deciding whether to push). Use name() for the enum string value;
// fall back to "unknown" if the value is null to avoid NullPointerException and missing attributes.
Span span = tracer.spanBuilder("gateway.ws_send")
        .setAttribute("message_type", messageType)
        .setAttribute("screen_context", screenContext != null ? screenContext.name() : "unknown")
        .setAttribute("delivered_sessions", sessionCount) // integer count, never a list
        .startSpan();
try (Scope scope = span.makeCurrent()) {
    // existing WebSocket send logic
} catch (Exception e) {
    span.recordException(e);
    span.setStatus(StatusCode.ERROR, e.getMessage());
    throw e;
} finally {
    span.end();   // must always execute — missing span.end() leaks memory
}
```

**`makeCurrent()`** is required — without it the span is not active in the call stack, so any child operations won't be parented to it. **`span.end()` in `finally`** is mandatory — a span that is never ended never appears in Tempo.

Use `GlobalOpenTelemetry.getTracer()` — do **not** inject `OpenTelemetry` as a Spring `@Bean`. Adding `opentelemetry-spring-boot-starter` alongside the agent causes double-instrumentation on HTTP and Kafka spans.

---

## Edge Cases

| Edge Case | Handling |
|---|---|
| Alloy or Tempo restart | **Tempo restart**: Alloy queue (10k batches) + 5-min retry absorbs it; zero span loss in practice. **Alloy restart**: SDK BSP queue (2048 spans, ~10–30s at normal load) absorbs it; spans beyond that are dropped. **Sustained Tempo outage (> 5 min)**: Alloy queue saturates, new spans are dropped (oldest preserved). Drop is silent by default — monitor `otelcol_exporter_queue_size` on Alloy `:12345/metrics` and alert at 80% (8000) to catch this before saturation. Loss is acceptable for traces; document threshold in runbook. |
| `KafkaCorrelationRecordInterceptor` vs OTel Kafka instrumentation | The interceptor reads `X-Correlation-Id`; OTel agent reads `traceparent`. Different headers, different MDC keys — zero conflict. |
| WebSocket delivery | Covered in Phase 8 — manual span wrapping the outbound send in `GatewayOutboundService`. |
| DLQ trace linking | A DLQ-bound span is a child of the batch consumer span (MDC/context is intact at the point of DLQ send). The trace correctly shows the failure path through to DLQ. |
| Health check noise | Handled in Alloy tail sampler (`drop-health-checks` policy). |
| `userId` in Kafka message keys | Stripped by Alloy attribute scrubber before indexing in Tempo. |
| Dev `bootRun` without docker | Set `OTEL_SDK_DISABLED=true` to run completely without tracing, or set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` with `docker compose up -d alloy tempo`. |
| Load testing (standard) | Reduce Alloy tail sampler baseline (e.g. `sampling_percentage = 1`). App stays `always_on` — Alloy controls storage cost without affecting in-process span creation overhead. |
| Load testing (CPU/memory emergency) | If in-process span creation overhead becomes measurable, temporarily override: `OTEL_TRACES_SAMPLER=parentbased_traceidratio`, `OTEL_TRACES_SAMPLER_ARG=0.1`. **Trade-off**: 90% of error and slow traces are now head-dropped before Alloy's "always keep" policies can act — the guarantee breaks. Use only as emergency relief, document the override in the runbook with a TTL, and revert to `always_on` when load normalizes. |
| `service.version` in traces | Set via `OTEL_RESOURCE_ATTRIBUTES=service.version=0.1.0` in docker-compose. Allows filtering traces by deployment version during rollouts. |

---

## What This Does NOT Cover (Future Phases)

- **Baggage propagation** for `userId` across service boundaries (if this splits into microservices)
- **Exemplar histogram linking for business metrics** (`business_*`) — requires custom `SpanContext` injection on those counters
- **Production collector HA** — Alloy in single-instance mode is not HA; production K8s deploys Alloy as a DaemonSet with a central Tempo cluster

---

## Files Changed Summary

| File | Action |
|---|---|
| `Dockerfile` | Add agent download + `-javaagent` ENTRYPOINT |
| `docker-compose.yml` | OTEL env vars; `always_on` sampler; `service.instance.id`; Alloy ports; Tempo service; Prometheus flags; volumes |
| `ops/tempo-config.yml` | **New** — Tempo single-binary config with `metrics_generator` |
| `ops/alloy/config.alloy` | Add OTLP receiver + attribute scrubber + tail sampler + batch + Tempo exporter with retry/queue |
| `src/main/resources/logback-spring.xml` | Add `traceId`/`spanId` using correct MDC keys `trace_id`/`span_id` |
| `ops/grafana/provisioning/datasources/prometheus.yml` | Add Tempo datasource with trace→log + trace→metrics + service map config |
| `ops/grafana/dashboards/latency-and-slo.json` | Set `"exemplar": true` on HTTP latency histogram panel |
| `src/main/java/.../common/config/AsyncConfig.java` | Wrap `pricingExecutor` with `Context.taskWrapping()` if orphan spans confirmed |
| `src/main/java/.../streaming/service/GatewayOutboundService.java` | Add `gateway.ws_send` manual span with `makeCurrent()` + `span.end()` lifecycle |
| `build.gradle.kts` | Add `compileOnly("io.opentelemetry:opentelemetry-api")` for WS span compilation |

---

## Verification Sequence

```bash
# 1. Start infra
docker compose up -d alloy tempo prometheus grafana loki

# 2. Verify Tempo is ready
curl http://localhost:3200/ready
# → "ready"

# 3. Build and start app with agent
docker compose up --build app

# 4. Confirm agent is active — look for this in startup logs:
# [otel.javaagent 2.9.0] ... INFO  OpenTelemetry agent started

# 5. Trigger a price tick
curl -X POST http://localhost:8080/api/admin/simulate/tick

# 6. Search for traces
# Grafana → Explore → Tempo → Search
# Filter: service.name = retail-live-pricing
# Expect: traces with full span chain

# 7. Verify span chain in a trace:
# HTTP handler → Kafka produce price-ticks
#   → Kafka consume price-ticks (PriceCacheService)
#     → Kafka produce price-updates
#       → Kafka consume price-updates (FanOutService)
#         → Kafka produce portfolio-calc-tasks (×N batches)
#           → Kafka consume portfolio-calc-tasks (PortfolioTaskConsumer)
#             → JDBC SELECT portfolio
#             → JDBC SELECT positions
#             → Redis GET price:{symbol}
#             → JDBC INSERT audit_events
#             → Kafka produce portfolio-updates

# 8. Click any span → "Logs for this span"
# Expect: Loki returns JSON log lines with matching traceId

# 9. Open Latency & SLO dashboard
# Expect: exemplar dots on histogram; clicking jumps to Tempo trace

# 10. Grafana → Explore → Tempo → Service Graph
# Expect: service dependency map generated by metrics_generator

# 11. Verify async spans (Phase 7 check)
# In a batch trace, all calculateAndPublish spans must be
# CHILDREN of the onBatchTask consumer span, not orphan roots.
# If orphans appear → apply Context.taskWrapping() fix in AsyncConfig.

# 12a. Verify Kafka trace propagation (header check)
# Open Kafka UI (localhost:5777) → portfolio-updates topic → any message
# → Headers tab → confirm traceparent header is present
# This validates OTel context propagation across Kafka, not the WS span.

# 12b. Verify WS delivery span (Tempo trace view)
# Grafana → Explore → Tempo → search service.name=retail-live-pricing
# → open any trace that includes portfolio-updates consumption
# → expand span tree to the final child → must be gateway.ws_send
# → click span → attributes must include message_type, screen_context, delivered_sessions
# → status must be OK (green); on send failure must show ERROR with recordException detail
```

---

### Phase 9: Production Maturity Enhancements (High-Signal Additions)

**Goal:** elevate observability from “working” to “operations-ready” with actionable alerts, safer cost/cardinality posture, and business-path diagnostics.

#### 9.1 SLO Burn-Rate Alerting (Multi-window)

**Files:** `ops/prometheus-alerts.yml`, `docs/runbooks/` (new)

Add burn-rate rules for:
- Availability error budget burn (`5m/1h` and `30m/6h`)
- Latency SLO budget burn (`5m/1h` and `30m/6h`)

Acceptance:
- Fast-burn alert fires during acute issue within minutes.
- Slow-burn alert fires during prolonged degradation.
- Alert payload links to dashboard + trace query + runbook.

#### 9.2 Alert-to-Runbook Contract

**Files:** `docs/runbooks/*.md`, alert annotations

For each paging alert, add:
- `summary`, `impact`, `first_checks`, `dashboard_links`, `rollback_criteria`
- concrete Loki and Tempo queries for triage

Acceptance:
- On-call can execute first-response steps without tribal knowledge.
- Every critical alert has a matching runbook page.

#### 9.3 Kafka Reliability Visibility

**Files:** Grafana dashboards + Prometheus rules

Track and alert on:
- Consumer lag by group/topic/partition
- Rebalance frequency and pause duration
- DLQ production rate and retry exhaustion

Acceptance:
- Lag spike and stuck-consumer scenarios are visible within one dashboard refresh window.
- Lag alert includes affected topic/group labels.

#### 9.4 Business Data-Quality Signals

**Files:** app metrics module, Grafana dashboard

Add metrics for:
- `business_stale_feed_ratio`
- `business_outlier_reject_rate`
- `business_conflation_drop_ratio`
- `business_portfolio_debounce_hit_ratio`

Acceptance:
- Operators can distinguish infra healthy vs market-data quality degraded.
- Business anomaly alerts can trigger even when CPU/memory are normal.

#### 9.5 Cardinality and Cost Guardrails

**Files:** Alloy config, metrics docs, dashboards

Enforce and monitor:
- no `userId`, `sessionId`, raw symbols list, or message keys in span/log labels
- top-N cardinality views for metric labels and log labels
- alarms when label-cardinality growth exceeds thresholds

Acceptance:
- Cardinality regressions are detected before backend cost/latency blowups.

#### 9.6 Golden Journey E2E Dashboard

**Files:** `ops/grafana/dashboards/golden-journey.json` (new)

Single-pane trace/metric view for:
`ingestion -> pricing-cache -> fanout -> portfolio -> gateway.ws_send`

Include:
- per-stage p50/p95 latency
- per-stage error rate
- links from stage panels to representative traces and logs

Acceptance:
- One dashboard can answer: where latency regressed, where errors started, and whether user delivery was impacted.

#### 9.7 Synthetic Canary

**Files:** canary job, alert rules, dashboard

Run periodic synthetic flow:
- emit known tick
- verify downstream portfolio update and WS delivery under SLA

Acceptance:
- Canary alert triggers when pipeline silently breaks even if infra looks healthy.
- Canary success/failure trend visible over time.

#### 9.8 Security and Compliance Telemetry

**Files:** app audit metrics/logs, dashboards

Add:
- auth failures by endpoint
- token validation failures
- privileged admin action audit counters

Acceptance:
- security anomalies are visible with same rigor as performance anomalies.

#### 9.9 Operational Readiness Exit Criteria

Promote to “production-ready observability” only when:
- All critical alerts are actionable and runbook-linked
- Golden journey dashboard is green under nominal load
- Synthetic canary stable for 7 consecutive days
- Cardinality guardrails pass in staging under load test
- Incident drill completed with MTTR target documented

