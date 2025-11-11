package com.nova.billing.preparation.domain.wireless.enricher;

import org.springframework.stereotype.Component;

import com.nova.billing.preparation.core.model.StagingData;
import com.nova.billing.preparation.core.model.TargetItem;
import com.nova.billing.preparation.core.spi.TargetEnricher;

@Component
public class VipFlagEnricher implements TargetEnricher {

    @Override
    public boolean supports(String domain) {
        return "WIRELESS".equals(domain);
    }

    @Override
    public void enrich(TargetItem target, StagingData rawData) {
        if ("VIP".equals(rawData.getGrade()) || "VVIP".equals(rawData.getGrade())) {
            target.addExtraInfo("isVip", true);
        } else {
            target.addExtraInfo("isVip", false);
        }
    }

}
