package com.retail.livepricing.common.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class KafkaCorrelationRecordInterceptor implements RecordInterceptor<String, Object> {

    @Override
    public ConsumerRecord<String, Object> intercept(ConsumerRecord<String, Object> record,
                                                    Consumer<String, Object> consumer) {
        Header header = record.headers().lastHeader(CorrelationContext.CORRELATION_ID_HEADER);
        if (header == null || header.value() == null) {
            CorrelationContext.set(CorrelationContext.newId());
        } else {
            CorrelationContext.set(new String(header.value(), StandardCharsets.UTF_8));
        }
        return record;
    }

    @Override
    public void success(ConsumerRecord<String, Object> record,
                        Consumer<String, Object> consumer) {
        CorrelationContext.clear();
    }

    @Override
    public void failure(ConsumerRecord<String, Object> record,
                        Exception exception,
                        Consumer<String, Object> consumer) {
        CorrelationContext.clear();
    }
}
