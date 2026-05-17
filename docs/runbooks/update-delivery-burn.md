# Runbook: Update Delivery Burn Alerts

## Alerts
- `UpdateDeliveryFastBurn`
- `UpdateDeliverySlowBurn`

## Impact
Live updates may be delayed or dropped for connected users.

## First Checks
1. Open Grafana latency dashboard: `Pricing / Latency & SLO`.
2. Check `business_updates_failed_total` vs `business_updates_delivered_total` rates.
3. In Loki, run:
   - `{compose_service="app"} | json | message =~ "FLOW stage=gateway.ws_send status=failed.*"`
4. In Tempo, filter service `retail-live-pricing` and span name `gateway.ws_send` for errors.

## Triage Queries
- Prometheus failure ratio (5m):
  - `(sum(rate(business_updates_failed_total[5m])) / clamp_min(sum(rate(business_updates_delivered_total[5m])) + sum(rate(business_updates_failed_total[5m])), 1))`

## Rollback Criteria
Rollback/disable latest release if:
- error ratio remains >10% for 10+ minutes, and
- issue started right after deployment.
