package com.retail.livepricing.common.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Conflation conflation,
        Validation validation,
        @Valid Fanout fanout
) {
    public record Conflation(Duration freeWindow, Duration standardWindow, Duration proWindow) {
    }

    public record Validation(BigDecimal stockOutlierPct, BigDecimal etfOutlierPct, BigDecimal cryptoOutlierPct) {
    }

    public record Fanout(@Min(1) int batchSize) {
    }
}
