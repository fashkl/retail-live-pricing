package com.retail.livepricing.portfolio.repo;

import com.retail.livepricing.portfolio.entity.PortfolioEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PortfolioRepository extends JpaRepository<PortfolioEntity, String> {
    Optional<PortfolioEntity> findByUserId(String userId);
}
