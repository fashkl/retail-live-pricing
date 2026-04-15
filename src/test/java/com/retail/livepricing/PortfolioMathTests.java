package com.retail.livepricing;

import com.retail.livepricing.portfolio.service.PortfolioMath;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PortfolioMathTests {

    @Test
    void calculatesMarketValueAndUnrealizedPnlWithDeterministicScale() {
        BigDecimal market = PortfolioMath.marketValue(new BigDecimal("15"), new BigDecimal("178.55"));
        BigDecimal pnl = PortfolioMath.unrealizedPnl(market, new BigDecimal("2550"));

        assertThat(market).isEqualByComparingTo("2678.25000000");
        assertThat(pnl).isEqualByComparingTo("128.25000000");
    }
}
