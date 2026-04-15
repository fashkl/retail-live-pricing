package com.retail.livepricing.portfolio.repo;

import com.retail.livepricing.portfolio.entity.AuditEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AuditEventRepository extends JpaRepository<AuditEventEntity, Long> {
}
