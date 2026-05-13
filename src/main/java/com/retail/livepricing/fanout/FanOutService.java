package com.retail.livepricing.fanout;

import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.PortfolioCalcTaskV1;
import com.retail.livepricing.common.event.PriceUpdateV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.observability.KafkaMessageFactory;
import com.retail.livepricing.common.observability.CorrelationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Set;

@Service
public class FanOutService {
    private static final Logger log = LoggerFactory.getLogger(FanOutService.class);
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;
    private final BusinessMetrics businessMetrics;

    public FanOutService(StringRedisTemplate redisTemplate,
                         KafkaTemplate<String, Object> kafkaTemplate,
                         KafkaTopicsProperties topics,
                         BusinessMetrics businessMetrics) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.businessMetrics = businessMetrics;
    }

    @KafkaListener(topics = "${app.kafka.topics.price-updates}", groupId = "fanout")
    public void onPriceUpdate(PriceUpdateV1 event) {
        String symbol = event.payload().symbol();
        log.info("FLOW stage=fanout.consume topic=price-updates symbol={} correlationId={}",
                symbol, CorrelationContext.get());
        Set<String> users = redisTemplate.opsForSet().members("holders:" + symbol);
        if (users == null || users.isEmpty()) {
            businessMetrics.recordZeroImpactTick();
            businessMetrics.recordImpactedUsersPerTick(0);
            return;
        }

        for (String userId : users) {
            PortfolioCalcTaskV1 task = new PortfolioCalcTaskV1(userId, Set.of(symbol), Instant.now());
            MDC.put("userId", userId);
            try {
                kafkaTemplate.send(KafkaMessageFactory.build(topics.portfolioCalcTasks(), userId, task));
                log.info("FLOW stage=fanout.publish topic=portfolio-calc-tasks symbol={} userId={} correlationId={}",
                        symbol, userId, CorrelationContext.get());
            } finally {
                MDC.remove("userId");
            }
        }

        int impactedUsers = users.size();
        businessMetrics.recordImpactedUsersPerTick(impactedUsers);
        businessMetrics.recordPortfolioCalcTasksCreated(impactedUsers);
        log.info("FLOW stage=fanout.publish_summary topic=portfolio-calc-tasks symbol={} impactedUsers={} correlationId={}",
                symbol, impactedUsers, CorrelationContext.get());
    }
}
