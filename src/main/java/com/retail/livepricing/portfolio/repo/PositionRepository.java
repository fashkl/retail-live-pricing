package com.retail.livepricing.portfolio.repo;

import com.retail.livepricing.portfolio.entity.PositionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PositionRepository extends JpaRepository<PositionEntity, Long> {
    List<PositionEntity> findByPortfolioId(String portfolioId);
}
