package com.retail.livepricing.portfolio.repo;

import com.retail.livepricing.portfolio.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<UserEntity, String> {
}
