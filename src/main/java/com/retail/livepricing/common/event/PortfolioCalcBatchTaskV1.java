package com.retail.livepricing.common.event;

import java.time.Instant;
import java.util.List;

public record PortfolioCalcBatchTaskV1(List<String> userIds, String symbol, Instant triggeredAt) {
}
