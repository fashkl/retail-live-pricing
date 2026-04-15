package com.retail.livepricing.ingestion.adapter;

import com.retail.livepricing.common.model.Tick;
import org.springframework.stereotype.Component;

import java.util.stream.Stream;

@Component
public class HttpVendorAdapter implements MarketDataAdapter {

    @Override
    public String sourceName() {
        return "http-vendor-stub";
    }

    @Override
    public Stream<Tick> stream() {
        return Stream.empty();
    }
}
