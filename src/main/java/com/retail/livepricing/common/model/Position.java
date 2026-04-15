package com.retail.livepricing.common.model;

import java.math.BigDecimal;

public record Position(
        String symbol,
        BigDecimal quantity,
        BigDecimal avgCost,
        BigDecimal costBasis
) {
}
