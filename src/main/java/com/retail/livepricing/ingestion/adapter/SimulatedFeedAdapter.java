package com.retail.livepricing.ingestion.adapter;

import com.retail.livepricing.common.model.AssetClass;
import com.retail.livepricing.common.model.Tick;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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

    private final int minSymbolsPerCycle;
    private final int maxSymbolsPerCycle;
    private final int burstChancePct;
    private final int burstExtraMin;
    private final int burstExtraMax;
    private final int volatilitySpikeChancePct;

    private final Map<String, BigDecimal> base = Map.of(
            "AAPL", new BigDecimal("185.00"),
            "MSFT", new BigDecimal("430.00"),
            "NVDA", new BigDecimal("930.00"),
            "AMD", new BigDecimal("190.00"),
            "GOOGL", new BigDecimal("170.00"),
            "BTCUSD", new BigDecimal("73500.00"),
            "ETHUSD", new BigDecimal("3650.00")
    );

    public SimulatedFeedAdapter(
            @Value("${app.ingestion.simulation.min-symbols-per-cycle:4}") int minSymbolsPerCycle,
            @Value("${app.ingestion.simulation.max-symbols-per-cycle:7}") int maxSymbolsPerCycle,
            @Value("${app.ingestion.simulation.burst-chance-pct:20}") int burstChancePct,
            @Value("${app.ingestion.simulation.burst-extra-min:2}") int burstExtraMin,
            @Value("${app.ingestion.simulation.burst-extra-max:7}") int burstExtraMax,
            @Value("${app.ingestion.simulation.volatility-spike-chance-pct:8}") int volatilitySpikeChancePct) {
        this.minSymbolsPerCycle = Math.max(1, minSymbolsPerCycle);
        this.maxSymbolsPerCycle = Math.max(this.minSymbolsPerCycle, maxSymbolsPerCycle);
        this.burstChancePct = clampPct(burstChancePct);
        this.burstExtraMin = Math.max(0, burstExtraMin);
        this.burstExtraMax = Math.max(this.burstExtraMin, burstExtraMax);
        this.volatilitySpikeChancePct = clampPct(volatilitySpikeChancePct);
    }

    @Override
    public String sourceName() {
        return "simulated-feed";
    }

    @Override
    public Stream<Tick> stream() {
        int count = randomInRange(minSymbolsPerCycle, maxSymbolsPerCycle);

        if (roll(burstChancePct)) {
            count += randomInRange(burstExtraMin, burstExtraMax);
        }

        List<String> picks = pickSymbols(count);
        return picks.stream().map(this::nextTick);
    }

    private Tick nextTick(String symbol) {
        BigDecimal b = base.get(symbol);

        // baseline micro-drift + occasional volatility spike for realistic rate/shape changes
        double baselineDrift = (random.nextDouble() - 0.5D) * 0.01D;
        double spike = roll(volatilitySpikeChancePct) ? (random.nextDouble() - 0.5D) * 0.08D : 0D;
        BigDecimal drift = BigDecimal.valueOf(baselineDrift + spike);

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

    private List<String> pickSymbols(int requestedCount) {
        if (requestedCount <= SYMBOLS.size()) {
            ArrayList<String> shuffled = new ArrayList<>(SYMBOLS);
            Collections.shuffle(shuffled, random);
            return shuffled.subList(0, requestedCount);
        }

        ArrayList<String> picks = new ArrayList<>(requestedCount);
        ArrayList<String> shuffled = new ArrayList<>(SYMBOLS);
        Collections.shuffle(shuffled, random);
        picks.addAll(shuffled);

        while (picks.size() < requestedCount) {
            picks.add(SYMBOLS.get(random.nextInt(SYMBOLS.size())));
        }
        return picks;
    }

    private int randomInRange(int min, int max) {
        if (max <= min) {
            return min;
        }
        return min + random.nextInt((max - min) + 1);
    }

    private boolean roll(int pct) {
        return random.nextInt(100) < pct;
    }

    private int clampPct(int value) {
        return Math.max(0, Math.min(100, value));
    }
}
