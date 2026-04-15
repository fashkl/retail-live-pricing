package com.retail.livepricing;

import com.retail.livepricing.common.config.AppProperties;
import com.retail.livepricing.common.model.UserTier;
import com.retail.livepricing.streaming.service.ConflationPolicyService;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ConflationPolicyServiceTests {

    @Test
    void returnsExpectedTierWindow() {
        ConflationPolicyService service = new ConflationPolicyService(new AppProperties(
                new AppProperties.Conflation(Duration.ofSeconds(1), Duration.ofMillis(200), Duration.ofMillis(100)),
                new AppProperties.Validation(new BigDecimal("0.10"), new BigDecimal("0.10"), new BigDecimal("0.50"))
        ));

        assertThat(service.windowFor(UserTier.FREE)).isEqualTo(Duration.ofSeconds(1));
        assertThat(service.windowFor(UserTier.STANDARD)).isEqualTo(Duration.ofMillis(200));
        assertThat(service.windowFor(UserTier.PRO)).isEqualTo(Duration.ofMillis(100));
    }
}
