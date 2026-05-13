package com.retail.livepricing.portfolio.service;

import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.PortfolioCalcBatchTaskV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.observability.CorrelationContext;
import com.retail.livepricing.common.observability.KafkaMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class PortfolioTaskConsumer {
    private static final Logger log = LoggerFactory.getLogger(PortfolioTaskConsumer.class);
    private final PortfolioCalculatorService calculatorService;
    private final BusinessMetrics businessMetrics;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;

    public PortfolioTaskConsumer(PortfolioCalculatorService calculatorService,
                                 BusinessMetrics businessMetrics,
                                 KafkaTemplate<String, Object> kafkaTemplate,
                                 KafkaTopicsProperties topics) {
        this.calculatorService = calculatorService;
        this.businessMetrics = businessMetrics;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-calc-tasks}",
                   groupId = "portfolio-calculator",
                   containerFactory = "portfolioCalcListenerContainerFactory")
    public void onBatchTask(PortfolioCalcBatchTaskV1 task) {
        log.info("FLOW stage=portfolio.consume_batch topic=portfolio-calc-tasks symbol={} batchSize={} correlationId={}",
                task.symbol(), task.userIds().size(), CorrelationContext.get());
        for (String userId : task.userIds()) {
            MDC.put("userId", userId);
            try {
                calculatorService.calculateAndPublish(userId);
                businessMetrics.recordPortfolioCalculation();
            } catch (Exception e) {
                log.error("FLOW stage=portfolio.calc_failed symbol={} userId={} error={}",
                        task.symbol(), userId, e.getMessage(), e);
                kafkaTemplate.send(KafkaMessageFactory.build(
                        topics.deadLetter(),
                        userId,
                        new PortfolioCalcBatchTaskV1(List.of(userId), task.symbol(), Instant.now())
                ));
            } finally {
                MDC.remove("userId");
            }
        }
    }
}
