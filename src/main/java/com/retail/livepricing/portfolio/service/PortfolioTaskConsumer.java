package com.retail.livepricing.portfolio.service;

import com.retail.livepricing.common.event.PortfolioCalcTaskV1;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PortfolioTaskConsumer {
    private final PortfolioCalculatorService calculatorService;
    private final BusinessMetrics businessMetrics;

    public PortfolioTaskConsumer(PortfolioCalculatorService calculatorService,
                                 BusinessMetrics businessMetrics) {
        this.calculatorService = calculatorService;
        this.businessMetrics = businessMetrics;
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-calc-tasks}", groupId = "portfolio-calculator")
    public void onTask(PortfolioCalcTaskV1 task) {
        calculatorService.calculateAndPublish(task.userId());
        businessMetrics.recordPortfolioCalculation();
    }
}
