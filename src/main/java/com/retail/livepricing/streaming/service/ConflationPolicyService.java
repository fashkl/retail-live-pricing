package com.retail.livepricing.streaming.service;

import com.retail.livepricing.common.config.AppProperties;
import com.retail.livepricing.common.model.UserTier;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class ConflationPolicyService {
    private final AppProperties appProperties;

    public ConflationPolicyService(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    public Duration windowFor(UserTier tier) {
        return switch (tier) {
            case FREE -> appProperties.conflation().freeWindow();
            case STANDARD -> appProperties.conflation().standardWindow();
            case PRO -> appProperties.conflation().proWindow();
        };
    }
}
