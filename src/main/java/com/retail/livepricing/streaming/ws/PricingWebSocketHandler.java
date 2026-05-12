package com.retail.livepricing.streaming.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.model.AppState;
import com.retail.livepricing.common.model.ScreenContext;
import com.retail.livepricing.common.model.ScreenType;
import com.retail.livepricing.streaming.service.GatewayOutboundService;
import com.retail.livepricing.streaming.service.SessionContextService;
import com.retail.livepricing.streaming.service.WebSocketSessionRegistry;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@Component
public class PricingWebSocketHandler extends TextWebSocketHandler {
    private final WebSocketSessionRegistry sessionRegistry;
    private final SessionContextService sessionContextService;
    private final GatewayOutboundService outboundService;
    private final ObjectMapper objectMapper;
    private final BusinessMetrics businessMetrics;

    public PricingWebSocketHandler(WebSocketSessionRegistry sessionRegistry,
                                   SessionContextService sessionContextService,
                                   GatewayOutboundService outboundService,
                                   ObjectMapper objectMapper,
                                   BusinessMetrics businessMetrics) {
        this.sessionRegistry = sessionRegistry;
        this.sessionContextService = sessionContextService;
        this.outboundService = outboundService;
        this.objectMapper = objectMapper;
        this.businessMetrics = businessMetrics;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String userId = userId(session);
        sessionRegistry.register(userId, session);
        sessionContextService.upsert(userId, ScreenType.PORTFOLIO, Set.of());
        outboundService.sendStatus(userId, "connected", "stream_ready");
        businessMetrics.recordWsConnectionOpened();
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        JsonNode json = objectMapper.readTree(message.getPayload());
        String type = json.path("type").asText("unknown");
        String userId = userId(session);

        switch (type) {
            case "screen_context" -> {
                ScreenType screen = ScreenType.valueOf(json.path("screen").asText("PORTFOLIO"));
                Set<String> symbols = new HashSet<>();
                JsonNode node = json.path("symbols");
                if (node.isArray()) {
                    for (JsonNode value : node) {
                        symbols.add(value.asText().toUpperCase());
                    }
                }
                sessionContextService.upsert(userId, screen, symbols);
                outboundService.sendStatus(userId, "ok", "screen_context_updated");
            }
            case "app_state" -> {
                AppState state = AppState.valueOf(json.path("state").asText("FOREGROUND"));
                ScreenContext current = sessionContextService.get(userId);
                if (current != null && current.appState() == AppState.BACKGROUND && state == AppState.FOREGROUND) {
                    businessMetrics.recordResumeCatchup(current.updatedAt(), Instant.now());
                }
                sessionContextService.setAppState(userId, state);
                outboundService.sendStatus(userId, "ok", "app_state_updated");
            }
            case "heartbeat" -> outboundService.sendStatus(userId, "ok", "heartbeat_ack");
            default -> {
                businessMetrics.recordWsGapEvent("unsupported_message_type");
                outboundService.sendStatus(userId, "error", "unsupported_message_type");
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String userId = userId(session);
        sessionRegistry.unregister(userId);
        sessionContextService.remove(userId);
        businessMetrics.recordWsConnectionClosed();
    }

    private String userId(WebSocketSession session) {
        String fromQuery = null;
        if (session.getUri() != null && session.getUri().getQuery() != null) {
            String[] parts = session.getUri().getQuery().split("&");
            for (String part : parts) {
                if (part.startsWith("userId=")) {
                    fromQuery = part.substring("userId=".length());
                    break;
                }
            }
        }
        return (fromQuery == null || fromQuery.isBlank()) ? "demo-user-1" : fromQuery;
    }
}
