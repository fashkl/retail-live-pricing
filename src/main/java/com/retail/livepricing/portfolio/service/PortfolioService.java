package com.retail.livepricing.portfolio.service;

import com.retail.livepricing.common.model.PortfolioSnapshot;
import org.springframework.stereotype.Service;

@Service
public class PortfolioService {
    private final PortfolioCalculatorService portfolioCalculatorService;

    public PortfolioService(PortfolioCalculatorService portfolioCalculatorService) {
        this.portfolioCalculatorService = portfolioCalculatorService;
    }

    public PortfolioSnapshot currentSnapshot(String userId) {
        return portfolioCalculatorService.calculateAndPublish(userId);
    }
}
