# Runbook: Security Telemetry (Phase 9 scaffold)

## Objective
Track authentication failures, token validation failures, and privileged actions with the same rigor as latency and errors.

## Planned Metrics
- `security_auth_failures_total{endpoint,reason}`
- `security_token_validation_failures_total{reason}`
- `security_admin_actions_total{action,outcome}`

## Initial Implementation Guidance
1. Add counters in security filters/handlers and admin controllers.
2. Keep labels low-cardinality (`reason`, `endpoint_group`, `action`).
3. Add dashboard panels + warning alerts on baseline deviation.

## Status
Scaffolded in runbook/docs; metric instrumentation to be added in next iteration.
