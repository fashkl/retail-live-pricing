package com.retail.livepricing.common.message;

import com.retail.livepricing.common.model.PriceUpdate;

import java.time.Instant;
import java.util.List;

public record PriceBatchMessage(
        String type,
        Instant serverTime,
        List<PriceUpdate> updates
) {
    public static PriceBatchMessage of(List<PriceUpdate> updates) {
        return new PriceBatchMessage("price_batch", Instant.now(), updates);
    }
}
