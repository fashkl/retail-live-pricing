package com.retail.livepricing.portfolio.service;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class PortfolioMath {
    private PortfolioMath() {
    }

    public static BigDecimal marketValue(BigDecimal quantity, BigDecimal lastPrice) {
        return quantity.multiply(lastPrice).setScale(8, RoundingMode.HALF_UP);
    }

    public static BigDecimal unrealizedPnl(BigDecimal marketValue, BigDecimal costBasis) {
        return marketValue.subtract(costBasis).setScale(8, RoundingMode.HALF_UP);
    }
}
