package com.retail.livepricing.common.metrics;

import com.retail.livepricing.common.model.AssetClass;
import com.retail.livepricing.common.model.ScreenType;
import com.retail.livepricing.common.model.Tick;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class MicrometerBusinessMetrics implements BusinessMetrics {
    private final MeterRegistry meterRegistry;
    private final MetricTagPolicy tagPolicy;
    private final DistributionSummary impactedUsersSummary;

    public MicrometerBusinessMetrics(MeterRegistry meterRegistry, MetricTagPolicy tagPolicy) {
        this.meterRegistry = meterRegistry;
        this.tagPolicy = tagPolicy;
        this.impactedUsersSummary = DistributionSummary.builder(BusinessMetricCatalog.IMPACTED_USERS_PER_TICK)
                .description("Impacted users per price update tick")
                .baseUnit("users")
                .register(meterRegistry);
    }

    @Override
    public void recordTickPublished(Tick tick) {
        counter(BusinessMetricCatalog.TICKS_PUBLISHED_TOTAL, Map.of(
                "source", safe(tick.source()),
                "asset_class", safe(tick.assetClass().name()),
                "outcome", "published"
        ));
    }

    @Override
    public void recordTickRejected(String reason, String source) {
        counter(BusinessMetricCatalog.TICKS_REJECTED_TOTAL, Map.of(
                "reason", safe(reason),
                "source", safe(source),
                "outcome", "rejected"
        ));
    }

    @Override
    public void recordConflationInput(Tick tick) {
        counter(BusinessMetricCatalog.CONFLATION_INPUT_TOTAL, Map.of(
                "asset_class", safe(tick.assetClass().name()),
                "source", safe(tick.source())
        ));
    }

    @Override
    public void recordConflationFlush(int inputCount, int publishedCount) {
        int dropped = Math.max(0, inputCount - publishedCount);
        counter(BusinessMetricCatalog.CONFLATION_PUBLISHED_TOTAL, Map.of("outcome", "published"), publishedCount);
        if (dropped > 0) {
            counter(BusinessMetricCatalog.CONFLATION_DROPPED_TOTAL, Map.of("outcome", "dropped"), dropped);
        }
    }

    @Override
    public void recordPriceUpdatePublished(AssetClass assetClass, String source, boolean stale) {
        counter(BusinessMetricCatalog.PRICE_UPDATES_PUBLISHED_TOTAL, Map.of(
                "asset_class", safe(assetClass.name()),
                "source", safe(source),
                "outcome", "published"
        ));
        if (stale) {
            counter(BusinessMetricCatalog.STALE_UPDATES_TOTAL, Map.of(
                    "asset_class", safe(assetClass.name()),
                    "source", safe(source),
                    "reason", "stale"
            ));
        }
    }

    @Override
    public void recordImpactedUsersPerTick(int impactedUsers) {
        impactedUsersSummary.record(Math.max(0, impactedUsers));
    }

    @Override
    public void recordZeroImpactTick() {
        counter(BusinessMetricCatalog.ZERO_IMPACT_TICKS_TOTAL, Map.of("reason", "no_impacted_users"));
    }

    @Override
    public void recordPortfolioCalcTasksCreated(int tasks) {
        counter(BusinessMetricCatalog.PORTFOLIO_CALC_TASKS_CREATED_TOTAL, Map.of("outcome", "created"), Math.max(0, tasks));
    }

    @Override
    public void recordPortfolioCalculation() {
        counter(BusinessMetricCatalog.PORTFOLIO_CALCULATIONS_TOTAL, Map.of("outcome", "calculated"));
    }

    @Override
    public void recordPortfolioSnapshotPublished() {
        counter(BusinessMetricCatalog.PORTFOLIO_SNAPSHOTS_PUBLISHED_TOTAL, Map.of("outcome", "published"));
    }

    @Override
    public void recordPortfolioMissingPrice(String reason) {
        counter(BusinessMetricCatalog.PORTFOLIO_MISSING_PRICE_TOTAL, Map.of(
                "reason", safe(reason),
                "outcome", "fallback_used"
        ));
    }

    @Override
    public void recordTickToScreenLatency(Instant eventTime, ScreenType screen, String outcome) {
        recordLatency(BusinessMetricCatalog.TICK_TO_SCREEN_LATENCY_MS, eventTime, now(), Map.of(
                "stage", "tick_to_screen",
                "screen", safe(screen == null ? "UNKNOWN" : screen.name()),
                "outcome", safe(outcome)
        ));
    }

    @Override
    public void recordPortfolioRecalcDelay(Instant calculatedAt, String outcome) {
        recordLatency(BusinessMetricCatalog.PORTFOLIO_RECALC_DELAY_MS, calculatedAt, now(), Map.of(
                "stage", "portfolio_recalc",
                "outcome", safe(outcome)
        ));
    }

    @Override
    public void recordResumeCatchup(Instant backgroundAt, Instant resumedAt) {
        if (backgroundAt == null || resumedAt == null || resumedAt.isBefore(backgroundAt)) {
            return;
        }
        long ms = Duration.between(backgroundAt, resumedAt).toMillis();
        if (ms < 0) {
            return;
        }
        Timer.builder(BusinessMetricCatalog.RESUME_CATCHUP_MS)
                .tags(tagPolicy.sanitize(Map.of("stage", "resume", "outcome", "ok")))
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(ms, TimeUnit.MILLISECONDS);
    }

    @Override
    public void recordUpdateDelivered(String messageType, ScreenType screen) {
        Map<String, String> tags = new HashMap<>();
        tags.put("message_type", safe(messageType));
        tags.put("screen", safe(screen == null ? "UNKNOWN" : screen.name()));
        tags.put("outcome", "sent");
        counter(BusinessMetricCatalog.UPDATES_DELIVERED_TOTAL, tags);
    }

    @Override
    public void recordUpdateFailed(String messageType, ScreenType screen, String reason) {
        Map<String, String> tags = new HashMap<>();
        tags.put("message_type", safe(messageType));
        tags.put("screen", safe(screen == null ? "UNKNOWN" : screen.name()));
        tags.put("reason", safe(reason));
        tags.put("outcome", "failed");
        counter(BusinessMetricCatalog.UPDATES_FAILED_TOTAL, tags);
    }

    @Override
    public void recordWsConnectionOpened() {
        counter(BusinessMetricCatalog.WS_CONNECTIONS_OPENED_TOTAL, Map.of("outcome", "opened"));
    }

    @Override
    public void recordWsConnectionClosed() {
        counter(BusinessMetricCatalog.WS_CONNECTIONS_CLOSED_TOTAL, Map.of("outcome", "closed"));
    }

    @Override
    public void recordWsGapEvent(String reason) {
        counter(BusinessMetricCatalog.WS_GAP_EVENTS_TOTAL, Map.of(
                "reason", safe(reason),
                "outcome", "gap"
        ));
    }

    @Override
    public void recordFanOutBatchesEmitted(int batchCount) {
        counter(BusinessMetricCatalog.FANOUT_BATCHES_EMITTED_TOTAL, Map.of("outcome", "emitted"), batchCount);
    }

    private void counter(String metric, Map<String, String> tags) {
        meterRegistry.counter(metric, tagPolicy.sanitize(tags)).increment();
    }

    private void counter(String metric, Map<String, String> tags, int amount) {
        if (amount <= 0) {
            return;
        }
        meterRegistry.counter(metric, tagPolicy.sanitize(tags)).increment(amount);
    }

    private void recordLatency(String metric, Instant start, Instant end, Map<String, String> tags) {
        if (start == null || end == null || end.isBefore(start)) {
            return;
        }
        long ms = Duration.between(start, end).toMillis();
        if (ms < 0) {
            return;
        }
        Timer.builder(metric)
                .tags(tagPolicy.sanitize(tags))
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(ms, TimeUnit.MILLISECONDS);
    }

    private Instant now() {
        return Instant.now();
    }

    private String safe(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
