package com.retail.livepricing.portfolio.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.retail.livepricing.common.event.PortfolioSnapshotV1;
import com.retail.livepricing.common.model.PortfolioLine;
import com.retail.livepricing.common.model.PortfolioSnapshot;
import com.retail.livepricing.common.model.PriceUpdate;
import com.retail.livepricing.portfolio.entity.AuditEventEntity;
import com.retail.livepricing.portfolio.entity.PortfolioEntity;
import com.retail.livepricing.portfolio.entity.PositionEntity;
import com.retail.livepricing.portfolio.repo.AuditEventRepository;
import com.retail.livepricing.portfolio.repo.PortfolioRepository;
import com.retail.livepricing.portfolio.repo.PositionRepository;
import com.retail.livepricing.pricingcache.PriceCacheService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class PortfolioCalculatorService {
    private final PortfolioRepository portfolioRepository;
    private final PositionRepository positionRepository;
    private final AuditEventRepository auditEventRepository;
    private final PriceCacheService priceCacheService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final com.retail.livepricing.common.config.KafkaTopicsProperties topics;
    private final ObjectMapper objectMapper;

    public PortfolioCalculatorService(PortfolioRepository portfolioRepository,
                                      PositionRepository positionRepository,
                                      AuditEventRepository auditEventRepository,
                                      PriceCacheService priceCacheService,
                                      KafkaTemplate<String, Object> kafkaTemplate,
                                      com.retail.livepricing.common.config.KafkaTopicsProperties topics,
                                      ObjectMapper objectMapper) {
        this.portfolioRepository = portfolioRepository;
        this.positionRepository = positionRepository;
        this.auditEventRepository = auditEventRepository;
        this.priceCacheService = priceCacheService;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.objectMapper = objectMapper;
    }

    public PortfolioSnapshot calculateAndPublish(String userId) {
        PortfolioEntity portfolio = portfolioRepository.findByUserId(userId)
                .orElseThrow(() -> new IllegalArgumentException("Missing portfolio for user " + userId));
        List<PositionEntity> positions = positionRepository.findByPortfolioId(portfolio.getId());

        List<PortfolioLine> lines = new ArrayList<>();
        BigDecimal totalMarket = BigDecimal.ZERO;
        BigDecimal totalCost = BigDecimal.ZERO;

        for (PositionEntity p : positions) {
            PriceUpdate latest = priceCacheService.latestPrice(p.getSymbol());
            BigDecimal last = latest == null ? p.getAvgCost() : latest.last();
            BigDecimal marketValue = PortfolioMath.marketValue(p.getQuantity(), last);
            BigDecimal pnl = PortfolioMath.unrealizedPnl(marketValue, p.getCostBasis());

            lines.add(new PortfolioLine(
                    p.getSymbol(),
                    p.getQuantity(),
                    p.getAvgCost(),
                    last,
                    marketValue,
                    pnl
            ));
            totalMarket = totalMarket.add(marketValue);
            totalCost = totalCost.add(p.getCostBasis());
        }

        PortfolioSnapshot snapshot = new PortfolioSnapshot(
                userId,
                totalMarket,
                totalCost,
                totalMarket.subtract(totalCost),
                Instant.now(),
                lines
        );

        kafkaTemplate.send(topics.portfolioUpdates(), userId, new PortfolioSnapshotV1(snapshot));
        persistAudit(userId, snapshot);
        return snapshot;
    }

    private void persistAudit(String userId, PortfolioSnapshot snapshot) {
        AuditEventEntity event = new AuditEventEntity();
        event.setUserId(userId);
        event.setEventType("PORTFOLIO_SNAPSHOT");
        event.setCreatedAt(Instant.now());
        try {
            event.setPayloadJson(objectMapper.writeValueAsString(snapshot));
        } catch (JsonProcessingException ex) {
            event.setPayloadJson("{\"error\":\"serialization_failed\"}");
        }
        auditEventRepository.save(event);
    }
}
