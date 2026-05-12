package com.retail.livepricing.ingestion.service;

import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.TickV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.model.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TickPublisher {
    private static final Logger log = LoggerFactory.getLogger(TickPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;
    private final BusinessMetrics businessMetrics;

    public TickPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                         KafkaTopicsProperties topics,
                         BusinessMetrics businessMetrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.businessMetrics = businessMetrics;
    }

    public void publish(Tick tick) {
        kafkaTemplate.send(topics.priceTicks(), tick.symbol(), new TickV1(tick));
        businessMetrics.recordTickPublished(tick);
        log.debug("Published tick symbol={} seq={}", tick.symbol(), tick.sequence());
    }
}
