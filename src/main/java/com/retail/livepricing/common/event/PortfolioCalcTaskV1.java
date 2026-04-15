package com.retail.livepricing.common.event;

import java.time.Instant;
import java.util.Set;

public record PortfolioCalcTaskV1(
        String userId,
        Set<String> changedSymbols,
        Instant triggeredAt
) {
}
