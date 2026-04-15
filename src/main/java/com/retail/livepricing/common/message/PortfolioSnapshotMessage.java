package com.retail.livepricing.common.message;

import com.retail.livepricing.common.model.PortfolioSnapshot;

import java.time.Instant;

public record PortfolioSnapshotMessage(
        String type,
        Instant serverTime,
        PortfolioSnapshot snapshot
) {
    public static PortfolioSnapshotMessage of(PortfolioSnapshot snapshot) {
        return new PortfolioSnapshotMessage("portfolio_snapshot", Instant.now(), snapshot);
    }
}
