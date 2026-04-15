package com.retail.livepricing.portfolio.controller;

import com.retail.livepricing.common.model.PortfolioSnapshot;
import com.retail.livepricing.portfolio.service.PortfolioService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/portfolio")
public class PortfolioController {
    private final PortfolioService portfolioService;

    public PortfolioController(PortfolioService portfolioService) {
        this.portfolioService = portfolioService;
    }

    @GetMapping("/{userId}")
    public PortfolioSnapshot get(@PathVariable String userId) {
        return portfolioService.currentSnapshot(userId);
    }
}
