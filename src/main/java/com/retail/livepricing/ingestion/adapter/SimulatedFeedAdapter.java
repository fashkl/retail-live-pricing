package com.retail.livepricing.ingestion.adapter;

import com.retail.livepricing.common.model.AssetClass;
import com.retail.livepricing.common.model.Tick;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Component
public class SimulatedFeedAdapter implements MarketDataAdapter {
    private static final List<String> SYMBOLS = List.of("AAPL", "MSFT", "NVDA", "AMD", "GOOGL", "BTCUSD", "ETHUSD");
    private final Random random = new Random();
    private final AtomicLong sequence = new AtomicLong(1);
    private final Map<String, BigDecimal> base = Map.of(
            "AAPL", new BigDecimal("185.00"),
            "MSFT", new BigDecimal("430.00"),
            "NVDA", new BigDecimal("930.00"),
            "AMD", new BigDecimal("190.00"),
            "GOOGL", new BigDecimal("170.00"),
            "BTCUSD", new BigDecimal("73500.00"),
            "ETHUSD", new BigDecimal("3650.00")
    );

    @Override
    public String sourceName() {
        return "simulated-feed";
    }

    @Override
    public Stream<Tick> stream() {
        return SYMBOLS.stream().map(this::nextTick);
    }

    private Tick nextTick(String symbol) {
        BigDecimal b = base.get(symbol);
        BigDecimal drift = BigDecimal.valueOf((random.nextDouble() - 0.5D) * 0.01D);
        BigDecimal last = b.multiply(BigDecimal.ONE.add(drift)).setScale(4, RoundingMode.HALF_UP);
        BigDecimal bid = last.subtract(new BigDecimal("0.01")).max(BigDecimal.ZERO);
        BigDecimal ask = last.add(new BigDecimal("0.01"));
        AssetClass assetClass = symbol.endsWith("USD") ? AssetClass.CRYPTO : AssetClass.STOCK;
        return new Tick(
                symbol,
                assetClass,
                bid,
                ask,
                last,
                b,
                sequence.getAndIncrement(),
                Instant.now(),
                sourceName(),
                false
        );
    }
}
