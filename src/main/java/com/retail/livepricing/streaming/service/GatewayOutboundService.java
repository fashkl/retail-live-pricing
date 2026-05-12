package com.retail.livepricing.streaming.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retail.livepricing.common.event.PortfolioSnapshotV1;
import com.retail.livepricing.common.event.PriceUpdateV1;
import com.retail.livepricing.common.message.PortfolioSnapshotMessage;
import com.retail.livepricing.common.message.PriceBatchMessage;
import com.retail.livepricing.common.message.SystemStatusMessage;
import com.retail.livepricing.common.metrics.BusinessMetrics;
import com.retail.livepricing.common.model.AppState;
import com.retail.livepricing.common.model.PriceUpdate;
import com.retail.livepricing.common.model.ScreenContext;
import com.retail.livepricing.common.model.ScreenType;
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
    private final BusinessMetrics businessMetrics;

    public GatewayOutboundService(WebSocketSessionRegistry sessionRegistry,
                                  SessionContextService sessionContextService,
                                  ObjectMapper objectMapper,
                                  BusinessMetrics businessMetrics) {
        this.sessionRegistry = sessionRegistry;
        this.sessionContextService = sessionContextService;
        this.objectMapper = objectMapper;
        this.businessMetrics = businessMetrics;
    }

    @KafkaListener(topics = "${app.kafka.topics.price-updates}", groupId = "streaming-gateway")
    public void onPrice(PriceUpdateV1 event) {
        pushPriceToInterestedUsers(event.payload());
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
            PortfolioSnapshotMessage message = PortfolioSnapshotMessage.of(event.payload());
            if (send(session, message, "portfolio_snapshot", ctx.screen())) {
                businessMetrics.recordPortfolioRecalcDelay(event.payload().calculatedAt(), "sent");
            } else {
                businessMetrics.recordPortfolioRecalcDelay(event.payload().calculatedAt(), "failed");
            }
        });
    }

    public void sendStatus(String userId, String status, String detail) {
        sessionRegistry.find(userId).ifPresent(session -> {
            ScreenContext ctx = sessionContextService.get(userId);
            send(session, SystemStatusMessage.of(status, detail), "system_status", ctx == null ? ScreenType.PORTFOLIO : ctx.screen());
        });
    }

    private void pushPriceToInterestedUsers(PriceUpdate update) {
        PriceBatchMessage payload = PriceBatchMessage.of(List.of(update));
        for (var entry : sessionRegistry.entries()) {
            String userId = entry.getKey();
            WebSocketSession session = entry.getValue();
            ScreenContext ctx = sessionContextService.get(userId);
            if (ctx == null || ctx.appState() == AppState.BACKGROUND) {
                continue;
            }

            if (ctx.screen() == ScreenType.PORTFOLIO || includesSymbol(ctx.symbols(), update.symbol()) || ctx.screen() == ScreenType.STOCK_DETAIL) {
                boolean sent = send(session, payload, "price_batch", ctx.screen());
                businessMetrics.recordTickToScreenLatency(update.eventTime(), ctx.screen(), sent ? "sent" : "failed");
            }
        }
    }

    private boolean includesSymbol(Set<String> symbols, String symbol) {
        return symbols != null && symbols.contains(symbol);
    }

    private boolean send(WebSocketSession session, Object payload, String messageType, ScreenType screen) {
        try {
            session.sendMessage(new TextMessage(objectMapper.writeValueAsString(payload)));
            businessMetrics.recordUpdateDelivered(messageType, screen);
            return true;
        } catch (IOException ex) {
            businessMetrics.recordUpdateFailed(messageType, screen, "io_exception");
            return false;
        }
    }
}
