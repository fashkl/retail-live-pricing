package com.retail.livepricing.fanout;

import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.PortfolioCalcTaskV1;
import com.retail.livepricing.common.event.PriceUpdateV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Set;

@Service
public class FanOutService {
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
        Set<String> users = redisTemplate.opsForSet().members("holders:" + symbol);
        if (users == null || users.isEmpty()) {
            businessMetrics.recordZeroImpactTick();
            businessMetrics.recordImpactedUsersPerTick(0);
            return;
        }

        for (String userId : users) {
            PortfolioCalcTaskV1 task = new PortfolioCalcTaskV1(userId, Set.of(symbol), Instant.now());
            kafkaTemplate.send(topics.portfolioCalcTasks(), userId, task);
        }

        int impactedUsers = users.size();
        businessMetrics.recordImpactedUsersPerTick(impactedUsers);
        businessMetrics.recordPortfolioCalcTasksCreated(impactedUsers);
    }
}
