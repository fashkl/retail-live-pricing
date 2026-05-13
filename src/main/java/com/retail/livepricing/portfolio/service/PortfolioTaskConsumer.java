package com.retail.livepricing.portfolio.service;

import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.PortfolioCalcBatchTaskV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.observability.CorrelationContext;
import com.retail.livepricing.common.observability.KafkaMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

@Service
public class PortfolioTaskConsumer {
    private static final Logger log = LoggerFactory.getLogger(PortfolioTaskConsumer.class);
    private final PortfolioCalculatorService calculatorService;
    private final BusinessMetrics businessMetrics;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;
    private final Executor pricingExecutor;

    public PortfolioTaskConsumer(PortfolioCalculatorService calculatorService,
                                 BusinessMetrics businessMetrics,
                                 KafkaTemplate<String, Object> kafkaTemplate,
                                 KafkaTopicsProperties topics,
                                 @Qualifier("pricingExecutor") Executor pricingExecutor) {
        this.calculatorService = calculatorService;
        this.businessMetrics = businessMetrics;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.pricingExecutor = pricingExecutor;
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-calc-tasks}",
                   groupId = "portfolio-calculator",
                   containerFactory = "portfolioCalcListenerContainerFactory")
    public void onBatchTask(PortfolioCalcBatchTaskV1 task) {
        final String batchCorrelationId = CorrelationContext.getOrCreate();
        final String symbol = task.symbol();

        log.info("FLOW stage=portfolio.consume_batch topic=portfolio-calc-tasks symbol={} batchSize={} correlationId={}",
                symbol, task.userIds().size(), batchCorrelationId);

        List<CompletableFuture<Void>> futures = new ArrayList<>(task.userIds().size());
        for (String userId : task.userIds()) {
            try {
                futures.add(CompletableFuture.runAsync(() -> {
                    MDC.put(CorrelationContext.CORRELATION_ID_KEY, batchCorrelationId);
                    MDC.put("userId", userId);
                    try {
                        calculatorService.calculateAndPublish(userId);
                        businessMetrics.recordPortfolioCalculation();
                    } catch (Exception e) {
                        log.error("FLOW stage=portfolio.calc_failed symbol={} userId={} error={}",
                                symbol, userId, e.getMessage(), e);
                        kafkaTemplate.send(KafkaMessageFactory.build(
                                topics.deadLetter(),
                                userId,
                                new PortfolioCalcBatchTaskV1(List.of(userId), symbol, Instant.now())
                        ));
                    } finally {
                        MDC.remove("userId");
                        MDC.remove(CorrelationContext.CORRELATION_ID_KEY);
                    }
                }, pricingExecutor));
            } catch (RejectedExecutionException e) {
                log.error("FLOW stage=portfolio.submit_rejected symbol={} userId={} correlationId={}",
                        symbol, userId, batchCorrelationId, e);
                kafkaTemplate.send(KafkaMessageFactory.build(
                        topics.deadLetter(),
                        userId,
                        new PortfolioCalcBatchTaskV1(List.of(userId), symbol, Instant.now())
                ));
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("FLOW stage=portfolio.batch_complete symbol={} batchSize={} correlationId={}",
                symbol, task.userIds().size(), batchCorrelationId);
    }
}
