package com.retail.livepricing.ingestion.service;

import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.ingestion.adapter.SimulatedFeedAdapter;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SimulatedFeedIngestionJob {
    private final SimulatedFeedAdapter simulatedFeedAdapter;
    private final TickValidator tickValidator;
    private final TickPublisher tickPublisher;
    private final BusinessMetrics businessMetrics;
    private final FeedHealthService feedHealthService;

    public SimulatedFeedIngestionJob(SimulatedFeedAdapter simulatedFeedAdapter,
                                     TickValidator tickValidator,
                                     TickPublisher tickPublisher,
                                     BusinessMetrics businessMetrics,
                                     FeedHealthService feedHealthService) {
        this.simulatedFeedAdapter = simulatedFeedAdapter;
        this.tickValidator = tickValidator;
        this.tickPublisher = tickPublisher;
        this.businessMetrics = businessMetrics;
        this.feedHealthService = feedHealthService;
    }

    @Scheduled(fixedDelayString = "${app.ingestion.simulated-interval-ms:200}")
    public void ingest() {
        simulatedFeedAdapter.stream().forEach(tick -> {
            if (tickValidator.isValid(tick)) {
                tickPublisher.publish(tick);
                feedHealthService.markTick(tick.source());
            } else {
                businessMetrics.recordTickRejected("invalid_or_outlier", tick.source());
            }
        });
    }
}
