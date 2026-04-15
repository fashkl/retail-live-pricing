package com.retail.livepricing.ingestion.service;

import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.TickV1;
import com.retail.livepricing.common.model.Tick;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TickPublisher {
    private static final Logger log = LoggerFactory.getLogger(TickPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;
    private final MeterRegistry meterRegistry;

    public TickPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                         KafkaTopicsProperties topics,
                         MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.meterRegistry = meterRegistry;
    }

    public void publish(Tick tick) {
        kafkaTemplate.send(topics.priceTicks(), tick.symbol(), new TickV1(tick));
        meterRegistry.counter("ticks.published", "source", tick.source()).increment();
        log.debug("Published tick symbol={} seq={}", tick.symbol(), tick.sequence());
    }
}
