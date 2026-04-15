package com.retail.livepricing.common.message;

public sealed interface InboundMessage permits ScreenContextMessage, AppStateMessage, HeartbeatMessage {
    String type();
}
