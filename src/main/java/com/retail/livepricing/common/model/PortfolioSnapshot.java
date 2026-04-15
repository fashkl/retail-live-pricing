package com.retail.livepricing.common.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record PortfolioSnapshot(
        String userId,
        BigDecimal totalMarketValue,
        BigDecimal totalCostBasis,
        BigDecimal unrealizedPnl,
        Instant calculatedAt,
        List<PortfolioLine> lines
) {
}
