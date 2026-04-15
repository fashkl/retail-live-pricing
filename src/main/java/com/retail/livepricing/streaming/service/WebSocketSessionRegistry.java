package com.retail.livepricing.streaming.service;

import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WebSocketSessionRegistry {
    private final Map<String, WebSocketSession> sessionsByUser = new ConcurrentHashMap<>();

    public void register(String userId, WebSocketSession session) {
        sessionsByUser.put(userId, session);
    }

    public Optional<WebSocketSession> find(String userId) {
        return Optional.ofNullable(sessionsByUser.get(userId));
    }

    public void unregister(String userId) {
        sessionsByUser.remove(userId);
    }

    public Collection<Map.Entry<String, WebSocketSession>> entries() {
        return sessionsByUser.entrySet();
    }

    public int activeConnections() {
        return sessionsByUser.size();
    }
}
