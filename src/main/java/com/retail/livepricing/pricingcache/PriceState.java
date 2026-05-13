package com.retail.livepricing.pricingcache;

import com.retail.livepricing.common.model.PriceUpdate;
import com.retail.livepricing.common.model.Tick;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

public record PriceState(
        String symbol,
        PriceUpdate latest,
        Instant lastUpdatedAt,
        String correlationId
) {

    public static PriceState fromTick(Tick tick, String correlationId) {
        BigDecimal changeAmount = tick.last().subtract(tick.previousClose());
        BigDecimal changePercent = changeAmount
                .divide(tick.previousClose(), 8, RoundingMode.HALF_UP)
                .multiply(new BigDecimal("100"));

        PriceUpdate update = new PriceUpdate(
                tick.symbol(),
                tick.assetClass(),
                tick.bid(),
                tick.ask(),
                tick.last(),
                tick.previousClose(),
                changeAmount,
                changePercent,
                tick.eventTime(),
                tick.stale(),
                tick.source()
        );

        return new PriceState(tick.symbol(), update, Instant.now(), correlationId);
    }
}
