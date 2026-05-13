package com.retail.livepricing.common.metrics;

import com.retail.livepricing.common.model.AssetClass;
import com.retail.livepricing.common.model.ScreenType;
import com.retail.livepricing.common.model.Tick;

import java.time.Instant;

public interface BusinessMetrics {
    void recordTickPublished(Tick tick);

    void recordTickRejected(String reason, String source);

    void recordConflationInput(Tick tick);

    void recordConflationFlush(int inputCount, int publishedCount);

    void recordPriceUpdatePublished(AssetClass assetClass, String source, boolean stale);

    void recordImpactedUsersPerTick(int impactedUsers);

    void recordZeroImpactTick();

    void recordPortfolioCalcTasksCreated(int tasks);

    void recordPortfolioCalculation();

    void recordPortfolioSnapshotPublished();

    void recordPortfolioMissingPrice(String reason);

    void recordTickToScreenLatency(Instant eventTime, ScreenType screen, String outcome);

    void recordPortfolioRecalcDelay(Instant calculatedAt, String outcome);

    void recordResumeCatchup(Instant backgroundAt, Instant resumedAt);

    void recordUpdateDelivered(String messageType, ScreenType screen);

    void recordUpdateFailed(String messageType, ScreenType screen, String reason);

    void recordWsConnectionOpened();

    void recordWsConnectionClosed();

    void recordWsGapEvent(String reason);

    void recordFanOutBatchesEmitted(int batchCount);
}
