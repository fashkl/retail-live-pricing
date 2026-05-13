package com.retail.livepricing;

import com.retail.livepricing.common.config.AppProperties;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.model.AssetClass;
import com.retail.livepricing.common.model.Tick;
import com.retail.livepricing.common.observability.CorrelationContext;
import com.retail.livepricing.ingestion.adapter.SimulatedFeedAdapter;
import com.retail.livepricing.ingestion.service.FeedHealthService;
import com.retail.livepricing.ingestion.service.SimulatedFeedIngestionJob;
import com.retail.livepricing.ingestion.service.TickPublisher;
import com.retail.livepricing.ingestion.service.TickValidator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SimulatedFeedIngestionCorrelationTests {

    @Test
    void generatesDifferentCorrelationIdPerTickAndClearsMdcAfterIngest() {
        SimulatedFeedAdapter adapter = mock(SimulatedFeedAdapter.class);
        TickPublisher tickPublisher = mock(TickPublisher.class);
        BusinessMetrics businessMetrics = mock(BusinessMetrics.class);
        FeedHealthService feedHealthService = mock(FeedHealthService.class);

        AppProperties properties = new AppProperties(
                new AppProperties.Conflation(Duration.ofSeconds(1), Duration.ofMillis(200), Duration.ofMillis(100)),
                new AppProperties.Validation(new BigDecimal("0.5"), new BigDecimal("0.5"), new BigDecimal("0.5")),
                new AppProperties.Fanout(500)
        );
        TickValidator validator = new TickValidator(properties);

        Tick first = tick("AAPL", 1L);
        Tick second = tick("MSFT", 1L);
        when(adapter.stream()).thenReturn(Stream.of(first, second));

        List<String> correlationIds = new ArrayList<>();
        doAnswer(invocation -> {
            correlationIds.add(CorrelationContext.get());
            return null;
        }).when(tickPublisher).publish(any(Tick.class));

        SimulatedFeedIngestionJob job = new SimulatedFeedIngestionJob(
                adapter,
                validator,
                tickPublisher,
                businessMetrics,
                feedHealthService
        );

        job.ingest();

        assertNotNull(correlationIds.get(0));
        assertNotNull(correlationIds.get(1));
        assertNotEquals(correlationIds.get(0), correlationIds.get(1));
        assertNull(CorrelationContext.get());

        verify(tickPublisher, times(2)).publish(any(Tick.class));
        verify(feedHealthService, times(2)).markTick(any(String.class));
    }

    private static Tick tick(String symbol, long sequence) {
        BigDecimal previousClose = new BigDecimal("100.00");
        BigDecimal last = new BigDecimal("101.00");
        return new Tick(
                symbol,
                AssetClass.STOCK,
                last.subtract(new BigDecimal("0.01")),
                last.add(new BigDecimal("0.01")),
                last,
                previousClose,
                sequence,
                Instant.now(),
                "simulated-feed",
                false
        );
    }
}
