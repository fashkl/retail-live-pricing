package com.retail.livepricing.common.message;

import com.retail.livepricing.common.model.AppState;
import jakarta.validation.constraints.NotNull;

public record AppStateMessage(
        @NotNull AppState state
) implements InboundMessage {
    @Override
    public String type() {
        return "app_state";
    }
}
