package com.retail.livepricing.common.model;

import java.math.BigDecimal;

public record PortfolioLine(
        String symbol,
        BigDecimal quantity,
        BigDecimal avgCost,
        BigDecimal lastPrice,
        BigDecimal marketValue,
        BigDecimal unrealizedPnl
) {
}
