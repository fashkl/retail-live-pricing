package com.retail.livepricing.common.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class MetricTagPolicy {
    private static final Set<String> ALLOWED_TAG_KEYS = Set.of(
            "application",
            "env",
            "stage",
            "screen",
            "tier",
            "asset_class",
            "source",
            "outcome",
            "reason",
            "message_type"
    );

    private static final Set<String> FORBIDDEN_TAG_KEYS = Set.of(
            "userId",
            "user_id",
            "sessionId",
            "session_id",
            "symbol"
    );

    public Tags sanitize(Map<String, String> input) {
        List<Tag> tags = new ArrayList<>();
        for (var entry : input.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key == null || key.isBlank() || value == null || value.isBlank()) {
                continue;
            }
            if (FORBIDDEN_TAG_KEYS.contains(key)) {
                throw new IllegalArgumentException("Forbidden metric tag key: " + key);
            }
            if (!ALLOWED_TAG_KEYS.contains(key)) {
                continue;
            }

            String sanitized = value.trim();
            if (sanitized.length() > 64) {
                sanitized = sanitized.substring(0, 64);
            }
            tags.add(Tag.of(key, sanitized));
        }

        return Tags.of(tags);
    }
}
