# Observability Plan

## Goal
Make `retail-live-pricing` production-ready with full metrics, dashboards, alerts, logs, and traces, with clear rollout steps and test scenarios.

## Current Status (Done)
- Actuator + Prometheus endpoint exposed (`/actuator/prometheus` on management port).
- Business metrics exposed (`business_*`) and visible in Prometheus.
- Grafana dashboards provisioned:
  - Business Overview
  - Latency & SLO
  - Pipeline Health
- Kafka client metrics wired via Micrometer listeners.
- Kafka lag alert rules added:
  - `KafkaConsumerLagHigh`
  - `KafkaConsumerLagCritical`
- End-to-end lag incident + recovery tested.

## Step 1: Metrics Foundation (Completed)
- Standardize metric naming and tags.
- Enforce low-cardinality tag policy.
- Instrument ingestion, pricing, fanout, portfolio, and streaming paths.
- Validate metrics in Prometheus and Grafana.

## Step 2: Alerting (In Progress)
### Step 2A (Completed)
- Add Prometheus alert rules for business and pipeline SLOs.
- Add Kafka consumer lag alerts.
- Add lag panels to Pipeline Health dashboard.

### Step 2B (Partially Completed)
- Alertmanager config scaffold added (`ops/alertmanager.yml`).
- Prometheus wired to Alertmanager (`ops/prometheus.yml`).
- Alert runbook links added to all active rules.
- Runbooks created (`docs/runbooks/alerts.md`).
- Remaining work:
- Integrate real notification destinations/secrets.
- Route alerts by severity:
  - `warning` -> Slack
  - `critical` -> Slack + email/pager
- Add runbook links in alert annotations.
- Add grouping/inhibition to reduce alert noise.
- Validate fire + resolve notifications with game-day tests.

## Step 3: Structured Logging (In Progress)
- Completed:
- JSON logging format added (`logback-spring.xml`).
- Correlation ID support added:
  - HTTP `X-Correlation-Id` filter (request/response)
  - Kafka header propagation on producers
  - Kafka consumer interceptor restores correlation in MDC
- WS logging enriched with `userId` and `wsSessionId`.
- Remaining:
- Add centralized log storage/search (e.g., Loki or ELK) and saved queries.
- Add explicit log redaction policy for sensitive fields.

## Step 4: Distributed Tracing
- Add OpenTelemetry SDK and exporters.
- Trace path: ingestion -> Kafka -> pricing/fanout -> portfolio -> streaming gateway.
- Use Tempo or Jaeger locally first.
- Add exemplar links from Grafana metrics panels to traces.

## Step 5: SLOs and Error Budgets
- Finalize service-level indicators:
  - Tick-to-screen latency p95 < 500ms
  - Portfolio calc delay p95 target
  - WS delivery success ratio
  - Feed freshness / stale update rate
- Add error budget policy and escalation thresholds.

## Step 6: Reliability Playbooks
- Create runbooks per critical alert:
  - symptoms
  - probable causes
  - checks
  - mitigation actions
  - rollback/escalation path
- Add incident checklist for on-call simulation.

## Step 7: Portfolio/Showcase Evidence
- Capture dashboard screenshots under `docs/assets/observability/`.
- Add one incident timeline example (lag spike + recovery).
- Add README section: architecture + metrics + alerting + traces.

## Recommended Execution Order (Next 2 Iterations)
1. Step 2B Alert delivery + runbooks.
2. Step 3 Structured logging.
3. Step 4 Tracing (local first, then staging).

## Acceptance Checklist
- Prometheus target healthy and scraping continuously.
- Dashboards load with live data and no broken queries.
- Alerts fire and resolve with verified notifications.
- Logs include correlation IDs end-to-end.
- Traces visible for critical flows.
- README/doc pages updated with usage and troubleshooting.
