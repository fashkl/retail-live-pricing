package com.retail.livepricing.ingestion.service;

import com.retail.livepricing.common.config.AppProperties;
import com.retail.livepricing.common.model.AssetClass;
import com.retail.livepricing.common.model.Tick;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TickValidator {
    private final Map<String, Long> lastSequenceBySymbol = new ConcurrentHashMap<>();
    private final AppProperties appProperties;

    public TickValidator(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    public boolean isValid(Tick tick) {
        Long prevSeq = lastSequenceBySymbol.put(tick.symbol(), tick.sequence());
        if (prevSeq != null && tick.sequence() <= prevSeq) {
            return false;
        }

        if (tick.previousClose() == null || tick.previousClose().compareTo(BigDecimal.ZERO) <= 0) {
            return false;
        }

        BigDecimal threshold = thresholdForAsset(tick.assetClass());
        BigDecimal move = tick.last()
                .subtract(tick.previousClose())
                .abs()
                .divide(tick.previousClose(), 8, RoundingMode.HALF_UP);
        return move.compareTo(threshold) <= 0;
    }

    private BigDecimal thresholdForAsset(AssetClass assetClass) {
        return switch (assetClass) {
            case STOCK -> appProperties.validation().stockOutlierPct();
            case ETF -> appProperties.validation().etfOutlierPct();
            case CRYPTO -> appProperties.validation().cryptoOutlierPct();
        };
    }
}
