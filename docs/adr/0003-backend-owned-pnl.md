# ADR-0003: Backend Owns P&L Calculation

## Status
Accepted

## Context
Client-side P&L can diverge by floating-point behavior and lacks auditability.

## Decision
Compute portfolio P&L server-side using BigDecimal and publish snapshots.

## Consequences
- Deterministic financial calculations
- Audit trail for displayed values
- Enables server-side risk checks and alerts
