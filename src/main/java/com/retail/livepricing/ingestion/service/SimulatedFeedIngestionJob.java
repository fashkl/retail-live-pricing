package com.retail.livepricing.ingestion.service;

import com.retail.livepricing.ingestion.adapter.SimulatedFeedAdapter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SimulatedFeedIngestionJob {
    private final SimulatedFeedAdapter simulatedFeedAdapter;
    private final TickValidator tickValidator;
    private final TickPublisher tickPublisher;
    private final MeterRegistry meterRegistry;
    private final FeedHealthService feedHealthService;

    public SimulatedFeedIngestionJob(SimulatedFeedAdapter simulatedFeedAdapter,
                                     TickValidator tickValidator,
                                     TickPublisher tickPublisher,
                                     MeterRegistry meterRegistry,
                                     FeedHealthService feedHealthService) {
        this.simulatedFeedAdapter = simulatedFeedAdapter;
        this.tickValidator = tickValidator;
        this.tickPublisher = tickPublisher;
        this.meterRegistry = meterRegistry;
        this.feedHealthService = feedHealthService;
    }

    @Scheduled(fixedDelayString = "${app.ingestion.simulated-interval-ms:200}")
    public void ingest() {
        simulatedFeedAdapter.stream().forEach(tick -> {
            if (tickValidator.isValid(tick)) {
                tickPublisher.publish(tick);
                feedHealthService.markTick(tick.source());
            } else {
                meterRegistry.counter("ticks.rejected", "reason", "invalid_or_outlier").increment();
            }
        });
    }
}
