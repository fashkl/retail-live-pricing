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
import com.retail.livepricing.common.observability.CorrelationContext;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Service
public class GatewayOutboundService {
    private static final Logger log = LoggerFactory.getLogger(GatewayOutboundService.class);
    private static final String TRACER_NAME = "retail-live-pricing.streaming-gateway";
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
        log.info("FLOW stage=gateway.consume topic=price-updates event=price_update symbol={} stale={} correlationId={}",
                event.payload().symbol(), event.payload().stale(), CorrelationContext.get());
        pushPriceToInterestedUsers(event.payload());
    }

    @KafkaListener(topics = "${app.kafka.topics.portfolio-updates}", groupId = "streaming-gateway")
    public void onPortfolio(PortfolioSnapshotV1 event) {
        String userId = event.payload().userId();
        log.info("FLOW stage=gateway.consume topic=portfolio-updates event=portfolio_snapshot userId={} lines={} correlationId={}",
                userId, event.payload().lines().size(), CorrelationContext.get());
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
        int delivered = 0;
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
                if (sent) {
                    delivered++;
                }
            }
        }
        log.info("FLOW stage=gateway.fanout event=price_batch symbol={} deliveredSessions={} correlationId={}",
                update.symbol(), delivered, CorrelationContext.get());
    }

    private boolean includesSymbol(Set<String> symbols, String symbol) {
        return symbols != null && symbols.contains(symbol);
    }

    private boolean send(WebSocketSession session, Object payload, String messageType, ScreenType screen) {
        Tracer tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        Span span = tracer.spanBuilder("gateway.ws_send").startSpan();
        span.setAttribute("message_type", messageType);
        span.setAttribute("screen_context", screen == null ? "UNKNOWN" : screen.name());
        span.setAttribute("delivered_sessions", 0);
        try {
            try (Scope ignored = span.makeCurrent()) {
                MDC.put("wsSessionId", session.getId());
                String userId = userIdForSession(session);
                if (userId != null) {
                    MDC.put("userId", userId);
                }
                session.sendMessage(new TextMessage(objectMapper.writeValueAsString(payload)));
                span.setAttribute("delivered_sessions", 1);
                businessMetrics.recordUpdateDelivered(messageType, screen);
                log.info("FLOW stage=gateway.ws_send status=delivered type={} screen={} correlationId={}",
                        messageType, screen, CorrelationContext.get());
                return true;
            }
        } catch (IOException ex) {
            span.recordException(ex);
            span.setStatus(StatusCode.ERROR, ex.getMessage());
            businessMetrics.recordUpdateFailed(messageType, screen, "io_exception");
            log.warn("FLOW stage=gateway.ws_send status=failed type={} screen={} error={} correlationId={}",
                    messageType, screen, ex.getMessage(), CorrelationContext.get());
            return false;
        } finally {
            span.end();
            MDC.remove("wsSessionId");
            MDC.remove("userId");
        }
    }

    private String userIdForSession(WebSocketSession session) {
        for (var entry : sessionRegistry.entries()) {
            if (entry.getValue().getId().equals(session.getId())) {
                return entry.getKey();
            }
        }
        return null;
    }
}
