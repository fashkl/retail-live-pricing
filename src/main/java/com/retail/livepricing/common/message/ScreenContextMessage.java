package com.retail.livepricing.common.message;

import com.retail.livepricing.common.model.ScreenType;
import jakarta.validation.constraints.NotNull;

import java.util.Set;

public record ScreenContextMessage(
        @NotNull ScreenType screen,
        Set<String> symbols
) implements InboundMessage {
    @Override
    public String type() {
        return "screen_context";
    }
}
