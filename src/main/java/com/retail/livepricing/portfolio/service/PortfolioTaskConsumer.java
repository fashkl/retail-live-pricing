package com.retail.livepricing.portfolio.service;

import com.retail.livepricing.common.event.PortfolioCalcTaskV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.observability.CorrelationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PortfolioTaskConsumer {
    private static final Logger log = LoggerFactory.getLogger(PortfolioTaskConsumer.class);
    private final PortfolioCalculatorService calculatorService;
    private final BusinessMetrics businessMetrics;

    public PortfolioTaskConsumer(PortfolioCalculatorService calculatorService,
                                 BusinessMetrics businessMetrics) {
        this.calculatorService = calculatorService;
        this.businessMetrics = businessMetrics;
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-calc-tasks}", groupId = "portfolio-calculator")
    public void onTask(PortfolioCalcTaskV1 task) {
        MDC.put("userId", task.userId());
        try {
            log.info("FLOW stage=portfolio.consume topic=portfolio-calc-tasks userId={} symbols={} correlationId={}",
                    task.userId(), task.changedSymbols(), CorrelationContext.get());
            calculatorService.calculateAndPublish(task.userId());
            businessMetrics.recordPortfolioCalculation();
        } finally {
            MDC.remove("userId");
        }
    }
}
