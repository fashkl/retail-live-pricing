package com.retail.livepricing.common.observability;

import org.slf4j.MDC;

import java.util.UUID;

public final class CorrelationContext {
    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_KEY = "correlationId";

    private CorrelationContext() {
    }

    public static String get() {
        return MDC.get(CORRELATION_ID_KEY);
    }

    public static String set(String correlationId) {
        String normalized = (correlationId == null || correlationId.isBlank()) ? newId() : correlationId;
        MDC.put(CORRELATION_ID_KEY, normalized);
        return normalized;
    }

    public static String getOrCreate() {
        String existing = get();
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        return set(newId());
    }

    public static void clear() {
        MDC.remove(CORRELATION_ID_KEY);
    }

    public static String newId() {
        return UUID.randomUUID().toString();
    }
}
