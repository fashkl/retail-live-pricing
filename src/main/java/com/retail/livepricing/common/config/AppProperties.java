package com.retail.livepricing.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.time.Duration;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Conflation conflation,
        Validation validation
) {
    public record Conflation(Duration freeWindow, Duration standardWindow, Duration proWindow) {
    }

    public record Validation(BigDecimal stockOutlierPct, BigDecimal etfOutlierPct, BigDecimal cryptoOutlierPct) {
    }
}
