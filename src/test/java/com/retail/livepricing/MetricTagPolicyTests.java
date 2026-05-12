package com.retail.livepricing;

import com.retail.livepricing.common.metrics.MetricTagPolicy;
import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetricTagPolicyTests {

    private final MetricTagPolicy policy = new MetricTagPolicy();

    @Test
    void shouldRejectForbiddenTagKeys() {
        assertThrows(IllegalArgumentException.class, () ->
                policy.sanitize(Map.of("userId", "123", "outcome", "sent"))
        );
    }

    @Test
    void shouldKeepOnlyAllowedTags() {
        Tags tags = policy.sanitize(Map.of(
                "outcome", "sent",
                "screen", "PORTFOLIO",
                "custom", "ignored"
        ));

        assertEquals(2, tags.stream().count());
    }
}
