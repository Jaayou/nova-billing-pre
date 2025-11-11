package com.nova.billing.preparation.core.spi;

import com.nova.billing.preparation.core.model.StagingData;
import com.nova.billing.preparation.core.model.TargetItem;

public interface TargetEnricher {
    boolean supports(String domain);
    void enrich(TargetItem target, StagingData rowData);
}
