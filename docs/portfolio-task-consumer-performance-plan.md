# Plan: Parallel Batch Processing in PortfolioTaskConsumer

## Context

`PortfolioTaskConsumer.onBatchTask()` processes up to 500 userIds from a `PortfolioCalcBatchTaskV1` message **sequentially**. For each user it calls `calculatorService.calculateAndPublish()`, which makes 2 DB reads + 1 DB write per user. At ~10ms per user in dev and 20-50ms in prod, a 500-user batch takes 5-25 seconds on a single consumer thread. With 12 consumer threads, 12 batches process in parallel but each batch is internally single-threaded — the bottleneck is intra-batch.

A `pricingExecutor` (`ThreadPoolTaskExecutor`, core=8, max=32, queue=10K) already exists in `AsyncConfig` but is not injected into `PortfolioTaskConsumer`.

---

## Concurrency Model

Use `CompletableFuture.runAsync(…, pricingExecutor)` — **not** virtual threads — because:
- Parallelism is already capped at 32 (executor max) before HikariCP becomes the bottleneck; virtual-thread parking benefit is marginal
- `pricingExecutor` is Spring-managed (graceful shutdown, metrics, backpressure via `queueCapacity`)
- Avoids Hibernate session / `@Transactional` ThreadLocal interactions that come with an unbounded virtual-thread executor

Per-batch throughput improvement: 500 sequential → 500/32 waves × ~10ms = **~157ms** (16× improvement on the hot path).

---

## Implementation — 2 files

### File 1: `PortfolioTaskConsumer.java`

**Changes:**
1. **Inject `pricingExecutor`** — add `@Qualifier("pricingExecutor") Executor pricingExecutor` constructor param, store as field.

2. **Capture batch context before the loop** — immediately inside `onBatchTask()`:
   ```java
   final String batchCorrelationId = CorrelationContext.get();
   final String symbol = task.symbol();
   ```
   `CorrelationContext.CORRELATION_ID_KEY` is `public static final` — safe to use directly.

3. **Replace sequential `for` loop with fan-out:**
   ```java
   List<CompletableFuture<Void>> futures = task.userIds().stream()
       .map(userId -> CompletableFuture.runAsync(() -> {
           MDC.put(CorrelationContext.CORRELATION_ID_KEY, batchCorrelationId);
           MDC.put("userId", userId);
           try {
               calculatorService.calculateAndPublish(userId);
               businessMetrics.recordPortfolioCalculation();
           } catch (Exception e) {
               log.error("FLOW stage=portfolio.calc_failed symbol={} userId={} error={}",
                       symbol, userId, e.getMessage(), e);
               kafkaTemplate.send(KafkaMessageFactory.build(
                       topics.deadLetter(), userId,
                       new PortfolioCalcBatchTaskV1(List.of(userId), symbol, Instant.now())
               ));
           } finally {
               MDC.remove("userId");
               MDC.remove(CorrelationContext.CORRELATION_ID_KEY);
           }
       }, pricingExecutor))
       .toList();
   ```
   - MDC is set **inside each lambda** (on the `pricing-N` thread), not on the listener thread
   - `kafkaTemplate` and `businessMetrics` are thread-safe singletons
   - DLQ send: `CorrelationContext.getOrCreate()` inside `KafkaMessageFactory.build()` reads the MDC value installed above

4. **Block the listener thread until all futures complete** — offset is NOT committed until this returns:
   ```java
   CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
   ```
   All lambdas catch-and-swallow exceptions (routing to DLQ), so `allOf().join()` never throws — offset commit always proceeds.

5. **Batch completion log** — after `join()`:
   ```java
   log.info("FLOW stage=portfolio.batch_complete symbol={} batchSize={} correlationId={}",
           symbol, task.userIds().size(), batchCorrelationId);
   ```

6. **New imports:** `java.util.concurrent.CompletableFuture`, `java.util.concurrent.Executor`, `org.springframework.beans.factory.annotation.Qualifier`

**What does NOT change:** `@KafkaListener` annotation (including `containerFactory`), `@Service`, class name, DLQ payload shape, `PortfolioCalculatorService`.

### File 2: `application.yaml`

Add under the existing `spring.datasource` stanza:
```yaml
hikari:
  maximum-pool-size: 64
```

Rationale: 12 listener threads × up to 32 concurrent tasks each = up to 384 concurrent connection requests. HikariCP default of 10 would immediately starve. 64 allows 12 batches × ~5 concurrent DB ops each with headroom. Connections are held for ~2-50ms so the queue drains quickly within HikariCP's 30s default timeout.

---

## Key Invariants Preserved

| Concern | How it's handled |
|---|---|
| Kafka offset commit | `allOf().join()` blocks listener thread; offset committed only after all 500 users done |
| MDC / correlation ID | Captured as local `String` before lambdas; installed/removed inside each lambda |
| `KafkaCorrelationRecordInterceptor.success()` clears listener MDC | Harmless — lambdas use their own captured `batchCorrelationId` string |
| DLQ on per-user failure | Identical to current code, now runs on `pricing-N` thread (thread-safe) |
| Graceful shutdown | `ThreadPoolTaskExecutor` honours Spring shutdown; `allOf().join()` ensures in-flight work completes |
| HikariCP starvation | `maximum-pool-size: 64` sized for 12 × ~5 concurrent ops with headroom |

---

## Critical Files

- `src/main/java/com/retail/livepricing/portfolio/service/PortfolioTaskConsumer.java` — primary change
- `src/main/resources/application.yaml` — HikariCP pool size
- `src/main/java/com/retail/livepricing/common/config/AsyncConfig.java` — read-only reference (`pricingExecutor` bean: core=8, max=32, queue=10K)
- `src/main/java/com/retail/livepricing/common/observability/CorrelationContext.java` — read-only reference (`CORRELATION_ID_KEY` public constant)

---

## Follow-up Opportunities (out of scope here)

- **Batch DB queries**: replace 2N per-user queries with `findAllByUserIdIn()` + `findAllByPortfolioIdIn()` in `PortfolioCalculatorService` — reduces 1,000 DB round-trips to 2 per batch
- **Async audit**: move `auditEventRepository.save()` out of the hot path (publish to a separate audit Kafka topic)
- **Redis `SMEMBERS` → `SSCAN`** in `FanOutService`: avoid materialising the full holder set in JVM heap

---

## Verification

1. `./gradlew test` — all existing unit tests must pass
2. `docker compose up -d` + `./gradlew bootRun`
3. Confirm logs show `stage=portfolio.batch_complete` with `correlationId` set correctly
4. `curl http://localhost:8080/actuator/prometheus | grep portfolio_calculations_total` — counter increments in burst rather than linear cadence
5. Thread dump during load: `pricing-N` threads active during batch processing; listener threads blocked at `allOf().join()`
