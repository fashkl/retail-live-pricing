package com.retail.livepricing.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka.topics")
public record KafkaTopicsProperties(
        String priceTicks,
        String priceUpdates,
        String portfolioCalcTasks,
        String portfolioUpdates,
        String deadLetter
) {
}
