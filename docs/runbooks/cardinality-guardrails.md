# Runbook: Cardinality Guardrails

## Alert
- `PrometheusSeriesCardinalityHigh`

## Impact
High series cardinality increases cost and slows queries.

## Guardrails
Never emit high-cardinality IDs as metric labels or log labels:
- `userId`
- `wsSessionId`
- raw symbol lists
- dynamic message keys

## First Checks
1. Check active series:
   - `prometheus_tsdb_head_series`
2. Identify top metrics by series count:
   - `topk(20, count by (__name__)({__name__=~".+"}))`
3. Review recent instrumentation changes for new labels.

## Mitigation
- Remove or bucket noisy labels.
- Move high-cardinality dimensions from labels to log body.
- Re-deploy and verify active series trend declines.
