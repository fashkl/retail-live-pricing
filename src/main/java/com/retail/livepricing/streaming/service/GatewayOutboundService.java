package com.retail.livepricing.streaming.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retail.livepricing.common.event.PortfolioSnapshotV1;
import com.retail.livepricing.common.event.PriceUpdateV1;
import com.retail.livepricing.common.message.PortfolioSnapshotMessage;
import com.retail.livepricing.common.message.PriceBatchMessage;
import com.retail.livepricing.common.message.SystemStatusMessage;
import com.retail.livepricing.common.model.AppState;
import com.retail.livepricing.common.model.ScreenContext;
import com.retail.livepricing.common.model.ScreenType;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Service
public class GatewayOutboundService {
    private final WebSocketSessionRegistry sessionRegistry;
    private final SessionContextService sessionContextService;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public GatewayOutboundService(WebSocketSessionRegistry sessionRegistry,
                                  SessionContextService sessionContextService,
                                  ObjectMapper objectMapper,
                                  MeterRegistry meterRegistry) {
        this.sessionRegistry = sessionRegistry;
        this.sessionContextService = sessionContextService;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = "${app.kafka.topics.price-updates}", groupId = "streaming-gateway")
    public void onPrice(PriceUpdateV1 event) {
        String symbol = event.payload().symbol();
        pushToInterestedUsers(symbol, PriceBatchMessage.of(List.of(event.payload())));
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-updates}", groupId = "streaming-gateway")
    public void onPortfolio(PortfolioSnapshotV1 event) {
        String userId = event.payload().userId();
        sessionRegistry.find(userId).ifPresent(session -> {
            ScreenContext ctx = sessionContextService.get(userId);
            if (ctx == null || ctx.appState() == AppState.BACKGROUND) {
                return;
            }
            if (ctx.screen() != ScreenType.PORTFOLIO) {
                return;
            }
            send(session, PortfolioSnapshotMessage.of(event.payload()));
        });
    }

    public void sendStatus(String userId, String status, String detail) {
        sessionRegistry.find(userId).ifPresent(session -> send(session, SystemStatusMessage.of(status, detail)));
    }

    private void pushToInterestedUsers(String symbol, Object payload) {
        for (var entry : sessionRegistry.entries()) {
            String userId = entry.getKey();
            WebSocketSession session = entry.getValue();
            ScreenContext ctx = sessionContextService.get(userId);
            if (ctx == null || ctx.appState() == AppState.BACKGROUND) {
                continue;
            }
            if (ctx.screen() == ScreenType.PORTFOLIO || includesSymbol(ctx.symbols(), symbol) || ctx.screen() == ScreenType.STOCK_DETAIL) {
                send(session, payload);
            }
        }
    }

    private boolean includesSymbol(Set<String> symbols, String symbol) {
        return symbols != null && symbols.contains(symbol);
    }

    private void send(WebSocketSession session, Object payload) {
        try {
            session.sendMessage(new TextMessage(objectMapper.writeValueAsString(payload)));
            meterRegistry.counter("gateway.messages.sent").increment();
        } catch (IOException ex) {
            meterRegistry.counter("gateway.messages.failed").increment();
        }
    }
}
