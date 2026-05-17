# Phase 9 Production Maturity Notes

This phase adds operational artifacts to make observability actionable, not only visible.

## Added
- Prometheus alert rules for:
  - SLO burn-rate style signals (fast/slow burn)
  - Tick-to-screen p95 breach
  - Kafka lag/rebalance instability
  - Data-quality drift (stale feed / conflation drop ratio)
  - Prometheus cardinality guardrail
- Runbooks under `docs/runbooks/` with first checks and triage queries.
- Golden journey dashboard JSON:
  - `ops/grafana/dashboards/golden-journey.json`
- Synthetic canary helper:
  - `ops/canary/synthetic-flow-check.sh`

## Compose/Prometheus wiring
- Prometheus now loads `ops/prometheus-alerts.yml` via `rule_files`.
- Docker compose mounts alert file into Prometheus container.

## Notes
- Security telemetry counters are scaffolded as runbook contract for next iteration.
- Some Kafka metric names may vary by Micrometer/Spring Kafka version; adjust if a query returns no series.
