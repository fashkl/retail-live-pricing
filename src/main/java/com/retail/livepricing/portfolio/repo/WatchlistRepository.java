package com.retail.livepricing.portfolio.repo;

import com.retail.livepricing.portfolio.entity.WatchlistEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WatchlistRepository extends JpaRepository<WatchlistEntity, Long> {
    List<WatchlistEntity> findByUserId(String userId);
}
