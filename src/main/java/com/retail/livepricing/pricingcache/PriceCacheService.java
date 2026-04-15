package com.retail.livepricing.pricingcache;

import com.retail.livepricing.common.config.KafkaTopicsProperties;
import com.retail.livepricing.common.event.PriceUpdateV1;
import com.retail.livepricing.common.event.TickV1;
import com.retail.livepricing.common.model.PriceUpdate;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PriceCacheService {
    private final Map<String, PriceState> inMemory = new ConcurrentHashMap<>();
    private final Map<String, PriceState> pendingBySymbol = new ConcurrentHashMap<>();

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topics;
    private final MeterRegistry meterRegistry;

    public PriceCacheService(StringRedisTemplate redisTemplate,
                             KafkaTemplate<String, Object> kafkaTemplate,
                             KafkaTopicsProperties topics,
                             MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = "${app.kafka.topics.price-ticks}", groupId = "price-cache")
    public void onTick(TickV1 event) {
        PriceState state = PriceState.fromTick(event.payload());
        inMemory.put(state.symbol(), state);
        pendingBySymbol.put(state.symbol(), state);
        redisTemplate.opsForValue().set("price:" + state.symbol(), state.latest().last().toPlainString());
    }

    @Scheduled(fixedDelayString = "${app.conflation.standard-window-ms:200}")
    public void flushConflatedUpdates() {
        if (pendingBySymbol.isEmpty()) {
            return;
        }

        ArrayList<PriceState> pending = new ArrayList<>(pendingBySymbol.values());
        pendingBySymbol.clear();

        for (PriceState state : pending) {
            PriceUpdate update = new PriceUpdate(
                    state.latest().symbol(),
                    state.latest().assetClass(),
                    state.latest().bid(),
                    state.latest().ask(),
                    state.latest().last(),
                    state.latest().previousClose(),
                    state.latest().changeAmount(),
                    state.latest().changePercent(),
                    Instant.now(),
                    state.latest().stale(),
                    state.latest().source()
            );
            kafkaTemplate.send(topics.priceUpdates(), update.symbol(), new PriceUpdateV1(update));
            meterRegistry.counter("price.updates.published").increment();
        }
    }

    public PriceUpdate latestPrice(String symbol) {
        PriceState state = inMemory.get(symbol);
        return state == null ? null : state.latest();
    }
}
