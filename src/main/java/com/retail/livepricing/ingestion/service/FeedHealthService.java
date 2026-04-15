package com.retail.livepricing.ingestion.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class FeedHealthService {
    private final Map<String, Instant> lastTickAt = new ConcurrentHashMap<>();

    public void markTick(String source) {
        lastTickAt.put(source, Instant.now());
    }

    public Map<String, Instant> snapshot() {
        return Map.copyOf(lastTickAt);
    }
}
