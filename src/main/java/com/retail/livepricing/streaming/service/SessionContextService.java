package com.retail.livepricing.streaming.service;

import com.retail.livepricing.common.model.AppState;
import com.retail.livepricing.common.model.ScreenContext;
import com.retail.livepricing.common.model.ScreenType;
import com.retail.livepricing.common.model.UserTier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SessionContextService {
    private final Map<String, ScreenContext> contexts = new ConcurrentHashMap<>();
    private final StringRedisTemplate redisTemplate;

    public SessionContextService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void upsert(String userId, ScreenType screen, Set<String> symbols) {
        ScreenContext current = contexts.get(userId);
        AppState state = current == null ? AppState.FOREGROUND : current.appState();
        UserTier tier = current == null ? UserTier.STANDARD : current.tier();
        ScreenContext context = new ScreenContext(userId, screen, normalizeSymbols(symbols), state, tier, Instant.now());
        contexts.put(userId, context);
        redisTemplate.opsForValue().set("session:" + userId, screen.name());
    }

    public void setAppState(String userId, AppState state) {
        ScreenContext current = contexts.get(userId);
        if (current == null) {
            contexts.put(userId, new ScreenContext(userId, ScreenType.PORTFOLIO, Set.of(), state, UserTier.STANDARD, Instant.now()));
            return;
        }
        contexts.put(userId, new ScreenContext(userId, current.screen(), current.symbols(), state, current.tier(), Instant.now()));
    }

    public ScreenContext get(String userId) {
        return contexts.get(userId);
    }

    public void remove(String userId) {
        contexts.remove(userId);
        redisTemplate.delete("session:" + userId);
    }

    private Set<String> normalizeSymbols(Set<String> symbols) {
        if (symbols == null || symbols.isEmpty()) {
            return Set.of();
        }
        Set<String> normalized = new HashSet<>();
        for (String symbol : symbols) {
            if (symbol != null && !symbol.isBlank()) {
                normalized.add(symbol.trim().toUpperCase(Locale.ROOT));
            }
        }
        return Set.copyOf(normalized);
    }
}
