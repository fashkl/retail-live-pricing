package com.retail.livepricing.streaming.config;

import com.retail.livepricing.streaming.ws.PricingWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
    private final PricingWebSocketHandler pricingWebSocketHandler;

    public WebSocketConfig(PricingWebSocketHandler pricingWebSocketHandler) {
        this.pricingWebSocketHandler = pricingWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(pricingWebSocketHandler, "/ws/pricing").setAllowedOriginPatterns("*");
    }
}
