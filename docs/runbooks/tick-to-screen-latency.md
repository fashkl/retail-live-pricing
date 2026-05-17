# Runbook: Tick-to-Screen Latency High

## Alert
- `TickToScreenP95HighFastBurn`

## Impact
Users see stale prices and delayed portfolio reflection.

## First Checks
1. Dashboard: `Pricing / Latency & SLO`.
2. Confirm p95 from metric:
   - `histogram_quantile(0.95, sum(rate(business_tick_to_screen_latency_ms_bucket[5m])) by (le))`
3. Check Kafka lag metrics for correlated spikes.
4. Check app logs for stage bottlenecks:
   - `{compose_service="app"} | json | message =~ "FLOW stage=(pricing-cache.publish|fanout.publish_batch|portfolio.consume|gateway.ws_send).*"`

## Tempo
Find traces around the spike window and inspect slow spans in:
- `kafka process price-updates`
- `kafka process portfolio-calc-tasks`
- `gateway.ws_send`

## Rollback Criteria
If p95 remains >500ms for 15+ minutes after mitigation and started after deployment, rollback to previous stable image.
