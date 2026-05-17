# Runbook: Kafka Lag and Rebalance

## Alerts
- `KafkaConsumerLagHigh`
- `KafkaConsumerRebalanceSpike`

## Impact
Delayed processing in `price-updates`, `portfolio-calc-tasks`, or `portfolio-updates`.

## First Checks
1. Verify broker health in Kafka UI (`http://localhost:5777`).
2. Inspect consumer group lag by topic/partition.
3. Check app logs:
   - `{compose_service="app"} | json | message =~ "FLOW stage=(fanout.consume|portfolio.consume|gateway.consume).*"`
4. Validate container resources (CPU throttling / memory pressure).

## Triage Queries
- `max by (topic, partition, spring_id) (kafka_consumer_records_lag_max)`
- `sum(increase(kafka_consumer_rebalance_total[10m]))`

## Mitigation
- Restart only impacted consumer container if lag is stuck.
- Reduce fan-out batch size if downstream saturation observed.
- Temporarily increase Kafka partitions for hot topics (non-local env only).
