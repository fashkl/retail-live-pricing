package com.retail.livepricing.common.message;

import java.time.Instant;

public record SystemStatusMessage(
        String type,
        Instant serverTime,
        String status,
        String detail
) {
    public static SystemStatusMessage of(String status, String detail) {
        return new SystemStatusMessage("system_status", Instant.now(), status, detail);
    }
}
