package com.retail.livepricing.ingestion.controller;

import com.retail.livepricing.ingestion.service.FeedHealthService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/admin/feeds")
public class IngestionAdminController {
    private final FeedHealthService feedHealthService;

    public IngestionAdminController(FeedHealthService feedHealthService) {
        this.feedHealthService = feedHealthService;
    }

    @GetMapping("/health")
    public Map<String, Instant> feedHealth() {
        return feedHealthService.snapshot();
    }
}
