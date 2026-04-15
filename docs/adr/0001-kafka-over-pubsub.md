# ADR-0001: Use Kafka for Event Backbone

## Status
Accepted

## Context
The pricing pipeline needs replay, retention, and independent consumer groups at scale.

## Decision
Use Kafka topics for price ticks, price updates, fan-out tasks, and portfolio snapshots.

## Consequences
- Added operational complexity
- Strong decoupling and replay capability
- Safer onboarding for new consumers (risk, analytics, compliance)
