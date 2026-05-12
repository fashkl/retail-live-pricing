# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start infrastructure (Postgres, Redis, Kafka, Kafka UI)
docker compose up -d postgres redis kafka kafka-ui

# Run application
./gradlew bootRun

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.retail.livepricing.PortfolioMathTests"

# Build fat JAR
./gradlew bootJar

# Alternative run via script
./scripts/run-local.sh
```

Infra defaults (local profile): Postgres on `localhost:5432` db `live_pricing`, Redis on `localhost:6379` password `SeCrET`, Kafka on `localhost:9092`, Kafka UI on `localhost:5777`.

Redis credentials can be overridden via `SPRING_DATA_REDIS_HOST`, `SPRING_DATA_REDIS_PORT`, `SPRING_DATA_REDIS_PASSWORD`.

## Architecture

This is a **modular monolith** — a single Spring Boot application organized into bounded modules that mirror the intended microservice split. The pipeline is fully event-driven over Kafka.

### Data flow

```
SimulatedFeedAdapter
  → TickValidator (outlier + sequence check)
  → TickPublisher → Kafka: price-ticks
    → PriceCacheService (consumes price-ticks, hot in-memory + Redis, @Scheduled conflation flush)
      → Kafka: price-updates
        ├─ FanOutService (resolves holders:{symbol} from Redis → emits PortfolioCalcTaskV1)
        │     → Kafka: portfolio-calc-tasks
        │         → PortfolioTaskConsumer → PortfolioCalculatorService (BigDecimal P&L, persists to Postgres)
        │               → Kafka: portfolio-updates
        │                   → GatewayOutboundService (pushes portfolio snapshot to WS session)
        └─ GatewayOutboundService (pushes price_batch to WS sessions interested in the symbol)
```

### Module responsibilities

| Package | Responsibility |
|---|---|
| `ingestion` | Adapter interface (`MarketDataAdapter`), simulated feed, `TickValidator`, `TickPublisher` |
| `pricingcache` | Hot in-memory + Redis price store, conflation flush via `@Scheduled` |
| `fanout` | Resolves `holders:{symbol}` Redis set → emits `PortfolioCalcTaskV1` per user |
| `portfolio` | `PortfolioCalculatorService` (BigDecimal P&L), JPA entities, Flyway, `DataSeeder` |
| `streaming` | WebSocket handler, session registry, screen context, `GatewayOutboundService` (dual Kafka listener) |
| `admin` | Runtime status/feed health REST stubs |
| `security` | JWT filter scaffold (header parsing only — not production-grade) |
| `common` | Kafka event records (`TickV1`, `PriceUpdateV1`, `PortfolioCalcTaskV1`, `PortfolioSnapshotV1`), domain models, config |

### Key design invariants

- **Backend-owned P&L**: all `BigDecimal` arithmetic lives in `PortfolioCalculatorService`/`PortfolioMath` — never client-side.
- **Screen-based subscriptions**: the WebSocket client sends `{"type":"screen_context","screen":"PORTFOLIO","symbols":[...]}`. The backend gates what it pushes based on `ScreenContext` + `AppState` (skip when `BACKGROUND`).
- **Conflation**: `PriceCacheService.flushConflatedUpdates()` runs on `app.conflation.standard-window-ms` (default 200ms) and drains `pendingBySymbol` as a batch — last-write-wins per symbol within the window. `ConflationPolicyService` resolves per-tier windows (`FREE`=1000ms, `STANDARD`=200ms, `PRO`=100ms).
- **Tick validation**: `TickValidator` rejects out-of-sequence ticks and outliers exceeding configurable `%` thresholds per `AssetClass` (STOCK/ETF/CRYPTO).
- **Redis key model**: `price:{symbol}` (latest price string), `holders:{symbol}` (set of userId), `session:{userId}` (context marker).

### Kafka topics and consumer groups

| Topic | Producer | Consumer (group) |
|---|---|---|
| `price-ticks` | `TickPublisher` | `PriceCacheService` (price-cache) |
| `price-updates` | `PriceCacheService` | `FanOutService` (fanout), `GatewayOutboundService` (streaming-gateway) |
| `portfolio-calc-tasks` | `FanOutService` | `PortfolioTaskConsumer` (portfolio-calculator) |
| `portfolio-updates` | `PortfolioCalculatorService` | `GatewayOutboundService` (streaming-gateway) |
| `pricing-dead-letter` | — | — |

### Configuration profiles

`application.yaml` → shared defaults, active profile `local`.  
Override files: `application-local.yaml`, `application-staging.yaml`, `application-prod.yaml`.  
Key tunables live under the `app:` prefix (conflation windows, outlier thresholds, Kafka topic names).

### Tests

Tests are pure unit tests (no Spring context, no containers):
- `PortfolioMathTests` — deterministic P&L arithmetic
- `ConflationPolicyServiceTests` — tier window resolution
- `TickValidatorTests` — sequence rejection and outlier thresholds
- `RetailLivePricingApplicationTests` — smoke (context loads)

Integration tests using Testcontainers (Kafka, Postgres) are wired in `build.gradle.kts` but not yet written.

### CI

`.github/workflows/ci.yml` runs `./gradlew clean test bootJar` on each push.
