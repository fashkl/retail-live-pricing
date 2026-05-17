# Runbook: Market Data Quality Degradation

## Alerts
- `StaleFeedRatioHigh`
- `ConflationDropRatioHigh`

## Impact
Infra may be healthy while market data quality is degraded.

## First Checks
1. Compare stale ratio and conflation drop ratio trends.
2. Inspect ingestion and cache logs:
   - `{compose_service="app"} | json | message =~ "FLOW stage=(ingestion.publish|pricing-cache.consume|pricing-cache.publish).*"`
3. Verify tick reject spikes:
   - `rate(business_ticks_rejected_total[5m])`

## Triage Queries
- stale ratio:
  - `sum(rate(business_stale_updates_total[10m])) / clamp_min(sum(rate(business_price_updates_published_total[10m])),1)`
- drop ratio:
  - `sum(rate(business_conflation_dropped_total[10m])) / clamp_min(sum(rate(business_conflation_input_total[10m])),1)`

## Mitigation
- Check upstream feed health/replay mode.
- Tune conflation window for current market volatility profile.
