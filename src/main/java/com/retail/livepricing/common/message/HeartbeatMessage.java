package com.retail.livepricing.common.message;

public record HeartbeatMessage() implements InboundMessage {
    @Override
    public String type() {
        return "heartbeat";
    }
}
