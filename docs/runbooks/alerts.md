# Alert Runbooks

This runbook maps each Prometheus alert to concrete triage and mitigation steps.
Structured logging baseline:
- Logs are JSON (`logback-spring.xml`).
- Correlation key is `correlationId` (also returned via `X-Correlation-Id` in HTTP responses).
- Additional fields include `userId` and `wsSessionId` when available.

## TickToScreenP95High
- Meaning: Tick-to-screen p95 latency is above 500ms for 5 minutes.
- Verify:
  - Check Grafana `Latency & SLO` dashboard.
  - Check `KafkaConsumerLagHigh` and `KafkaConsumerLagCritical` state.
  - Search logs by `correlationId` for slow request/stream paths and WS delivery lines.
- Mitigate:
  - Verify app CPU/memory pressure.
  - Verify Kafka broker health and consumer lag.
  - Reduce simulator burst settings in non-prod if needed.
- Escalate when: p99 alert also fires or user-facing latency exceeds SLO for >15m.

## TickToScreenP99Critical
- Meaning: Tail latency degradation is severe.
- Verify:
  - Compare p95 vs p99 on dashboard.
  - Inspect lag spikes and WS failure ratio.
- Mitigate:
  - Scale app replicas (or local resources in dev).
  - Temporarily increase conflation aggressiveness.
- Escalate when: alert persists beyond 10 minutes.

## PortfolioRecalcP95High
- Meaning: Portfolio calc consumers are not keeping pace.
- Verify:
  - Check portfolio-calc task lag and calc throughput panels.
- Mitigate:
  - Increase consumer concurrency.
  - Check DB latency and slow queries.
- Escalate when: lag continues rising for 10+ minutes.

## WsUpdateFailureRatioHigh
- Meaning: Outbound stream failures exceed 2%.
- Verify:
  - Check active WS sessions and gateway logs.
  - Correlate with target down or network instability.
  - Filter logs where `message` contains `WS outbound failed`.
- Mitigate:
  - Restart gateway instance if degraded.
  - Back off outbound push rates temporarily.
- Escalate when: ratio >5% for more than 5 minutes.

## StaleUpdatesSpike
- Meaning: Feed freshness degraded and stale updates increased.
- Verify:
  - Check ingestion heartbeat and stale counters.
- Mitigate:
  - Validate feed adapter health.
  - Fail over to simulator/replay in non-prod.
- Escalate when: stale rate does not recover in 10 minutes.

## ConflationDropRatioHigh
- Meaning: Too many input ticks are being dropped by conflation.
- Verify:
  - Compare input vs published rates on `Pipeline Health` dashboard.
- Mitigate:
  - Tune conflation windows by tier.
  - Review burst profile and load shape.
- Escalate when: ratio >50% for 15+ minutes.

## RetailLivePricingTargetDown
- Meaning: Prometheus cannot scrape app metrics endpoint.
- Verify:
  - Check app process/container health.
  - Check management port exposure and network routing.
  - Confirm scrape target from Prometheus (`/api/v1/targets`) and app bind address.
- Mitigate:
  - Restart app.
  - Validate `management.server.port` and firewall/network rules.
- Escalate when: service remains down >5 minutes.

## KafkaConsumerLagHigh
- Meaning: Consumer lag above warning threshold.
- Verify:
  - Inspect `Pipeline Health` lag panels.
  - Identify lagging consumer group/client_id.
- Mitigate:
  - Check consumer error/retry loops.
  - Increase concurrency for lagging consumer.
- Escalate when: warning persists >15 minutes.

## KafkaConsumerLagCritical
- Meaning: Consumer lag is high enough to impact user experience.
- Verify:
  - Confirm lag trend is increasing.
  - Correlate with latency alerts.
  - Trace one `correlationId` from producer-side publish logs to downstream consumer and gateway logs.
- Mitigate:
  - Prioritize consumer recovery (retry storm, poison messages).
  - Temporarily throttle producer burst load.
- Escalate when: critical persists >5 minutes.
