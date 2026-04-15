package com.retail.livepricing.admin;

import com.retail.livepricing.streaming.service.WebSocketSessionRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@RestController
@RequestMapping("/api/admin")
public class AdminController {
    private final WebSocketSessionRegistry registry;
    private final AtomicReference<String> symbolConfig = new AtomicReference<>("default");

    @Value("${app.conflation.standard-window-ms:200}")
    private long defaultConflationMs;

    public AdminController(WebSocketSessionRegistry registry) {
        this.registry = registry;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "service", "retail-live-pricing",
                "time", Instant.now().toString(),
                "activeWsConnections", registry.activeConnections(),
                "defaultConflationMs", defaultConflationMs,
                "symbolConfigProfile", symbolConfig.get()
        );
    }

    @PostMapping("/symbol-config")
    public Map<String, Object> symbolConfig(@RequestBody Map<String, String> payload) {
        String profile = payload.getOrDefault("profile", "default");
        symbolConfig.set(profile);
        return Map.of("updated", true, "profile", profile);
    }

    @PostMapping("/replay")
    public Map<String, Object> replay(@RequestBody Map<String, String> payload) {
        return Map.of(
                "requested", true,
                "range", payload.getOrDefault("range", "latest"),
                "note", "Replay control endpoint stub for pluggable ingestion adapters"
        );
    }
}
