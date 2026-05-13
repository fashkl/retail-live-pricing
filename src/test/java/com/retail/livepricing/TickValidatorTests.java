package com.retail.livepricing;

import com.retail.livepricing.common.config.AppProperties;
import com.retail.livepricing.common.model.AssetClass;
import com.retail.livepricing.common.model.Tick;
import com.retail.livepricing.ingestion.service.TickValidator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class TickValidatorTests {

    @Test
    void rejectsLargeOutlierMoveForStocks() {
        TickValidator validator = new TickValidator(new AppProperties(
                new AppProperties.Conflation(Duration.ofSeconds(1), Duration.ofMillis(200), Duration.ofMillis(100)),
                new AppProperties.Validation(new BigDecimal("0.10"), new BigDecimal("0.10"), new BigDecimal("0.50")),
                new AppProperties.Fanout(500)
        ));

        Tick outlier = new Tick(
                "AAPL",
                AssetClass.STOCK,
                new BigDecimal("178"),
                new BigDecimal("179"),
                new BigDecimal("250"),
                new BigDecimal("180"),
                1,
                Instant.now(),
                "test",
                false
        );

        assertThat(validator.isValid(outlier)).isFalse();
    }
}
