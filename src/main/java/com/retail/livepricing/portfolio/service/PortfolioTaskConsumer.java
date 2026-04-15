package com.retail.livepricing.portfolio.service;

import com.retail.livepricing.common.event.PortfolioCalcTaskV1;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PortfolioTaskConsumer {
    private final PortfolioCalculatorService calculatorService;
    private final MeterRegistry meterRegistry;

    public PortfolioTaskConsumer(PortfolioCalculatorService calculatorService,
                                 MeterRegistry meterRegistry) {
        this.calculatorService = calculatorService;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-calc-tasks}", groupId = "portfolio-calculator")
    public void onTask(PortfolioCalcTaskV1 task) {
        calculatorService.calculateAndPublish(task.userId());
        meterRegistry.counter("portfolio.calculations").increment();
    }
}
