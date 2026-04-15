package com.retail.livepricing.ingestion.adapter;

import com.retail.livepricing.common.model.Tick;

import java.util.stream.Stream;

public interface MarketDataAdapter {
    String sourceName();

    Stream<Tick> stream();
}
