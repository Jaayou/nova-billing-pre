package com.nova.billing.preparation.core.spi;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.nova.billing.preparation.core.model.PreparationParameter;
import com.nova.billing.preparation.core.model.TargetItem;

public interface PreparationManager {
    boolean supports(String domain);

    ItemReader<Object> createReader(PreparationParameter param);

    ItemProcessor<Object, TargetItem> createProcessor(PreparationParameter param);
}
