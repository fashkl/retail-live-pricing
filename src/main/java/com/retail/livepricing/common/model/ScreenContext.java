package com.retail.livepricing.common.model;

import java.time.Instant;
import java.util.Set;

public record ScreenContext(
        String userId,
        ScreenType screen,
        Set<String> symbols,
        AppState appState,
        UserTier tier,
        Instant updatedAt
) {
}
