package com.retail.livepricing.common.model;

import java.math.BigDecimal;
import java.time.Instant;

public record PriceUpdate(
        String symbol,
        AssetClass assetClass,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal last,
        BigDecimal previousClose,
        BigDecimal changeAmount,
        BigDecimal changePercent,
        Instant eventTime,
        boolean stale,
        String source
) {
}
