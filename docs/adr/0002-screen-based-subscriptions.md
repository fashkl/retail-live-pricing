# ADR-0002: Screen-Based Subscription Protocol

## Status
Accepted

## Context
Per-symbol subscriptions create high cardinality state and leak risk with navigation churn.

## Decision
Client sends current screen context over one WebSocket; server resolves symbols and push policy.

## Consequences
- Simpler client
- Centralized authorization and filtering
- Better control of fan-out and conflation per screen/tier
