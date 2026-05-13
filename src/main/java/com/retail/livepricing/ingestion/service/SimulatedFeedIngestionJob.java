package com.retail.livepricing.ingestion.service;

import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.observability.CorrelationContext;
import com.retail.livepricing.ingestion.adapter.SimulatedFeedAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SimulatedFeedIngestionJob {
    private static final Logger log = LoggerFactory.getLogger(SimulatedFeedIngestionJob.class);

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
            CorrelationContext.set(CorrelationContext.newId());
            try {
                if (tickValidator.isValid(tick)) {
                    log.debug("Simulator tick prepared symbol={} seq={} correlationId={}",
                            tick.symbol(), tick.sequence(), CorrelationContext.get());
                    tickPublisher.publish(tick);
                    feedHealthService.markTick(tick.source());
                } else {
                    businessMetrics.recordTickRejected("invalid_or_outlier", tick.source());
                }
            } finally {
                CorrelationContext.clear();
            }
        });
    }
}
