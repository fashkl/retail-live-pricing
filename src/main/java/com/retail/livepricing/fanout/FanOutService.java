package com.retail.livepricing.fanout;

import com.retail.livepricing.common.config.AppProperties;
import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.PortfolioCalcBatchTaskV1;
import com.retail.livepricing.common.event.PriceUpdateV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.observability.KafkaMessageFactory;
import com.retail.livepricing.common.observability.CorrelationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class FanOutService {
    private static final Logger log = LoggerFactory.getLogger(FanOutService.class);
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;
    private final BusinessMetrics businessMetrics;
    private final AppProperties appProperties;

    public FanOutService(StringRedisTemplate redisTemplate,
                         KafkaTemplate<String, Object> kafkaTemplate,
                         KafkaTopicsProperties topics,
                         BusinessMetrics businessMetrics,
                         AppProperties appProperties) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.businessMetrics = businessMetrics;
        this.appProperties = appProperties;
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

        List<String> userList = new ArrayList<>(users);
        int batchSize = appProperties.fanout().batchSize();
        int batchCount = 0;

        for (int i = 0; i < userList.size(); i += batchSize) {
            List<String> chunk = userList.subList(i, Math.min(i + batchSize, userList.size()));
            batchCount++;
            PortfolioCalcBatchTaskV1 batchTask = new PortfolioCalcBatchTaskV1(chunk, symbol, Instant.now());
            String batchKey = symbol + ":" + batchCount;
            kafkaTemplate.send(KafkaMessageFactory.build(topics.portfolioCalcTasks(), batchKey, batchTask));
            log.info("FLOW stage=fanout.publish_batch topic=portfolio-calc-tasks symbol={} batchIndex={} batchSize={} correlationId={}",
                    symbol, batchCount, chunk.size(), CorrelationContext.get());
        }

        int impactedUsers = userList.size();
        businessMetrics.recordImpactedUsersPerTick(impactedUsers);
        businessMetrics.recordPortfolioCalcTasksCreated(impactedUsers);
        businessMetrics.recordFanOutBatchesEmitted(batchCount);
        log.info("FLOW stage=fanout.publish_summary topic=portfolio-calc-tasks symbol={} impactedUsers={} batchCount={} correlationId={}",
                symbol, impactedUsers, batchCount, CorrelationContext.get());
    }
}
