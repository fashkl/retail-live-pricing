package com.retail.livepricing.common.model;

import java.math.BigDecimal;
import java.time.Instant;

public record Tick(
        String symbol,
        AssetClass assetClass,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal last,
        BigDecimal previousClose,
        long sequence,
        Instant eventTime,
        String source,
        boolean stale
) {
}
