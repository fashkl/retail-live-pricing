package com.retail.livepricing.common.observability;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

public final class KafkaMessageFactory {
    private KafkaMessageFactory() {
    }

    public static Message<Object> build(String topic, String key, Object payload) {
        return MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader(CorrelationContext.CORRELATION_ID_HEADER, CorrelationContext.getOrCreate())
                .build();
    }
}
