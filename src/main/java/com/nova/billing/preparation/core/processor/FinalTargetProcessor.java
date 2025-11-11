package com.nova.billing.preparation.core.processor;

import java.util.List;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nova.billing.preparation.core.model.StagingData;
import com.nova.billing.preparation.core.model.TargetItem;
import com.nova.billing.preparation.core.spi.TargetEnricher;

import lombok.RequiredArgsConstructor;

@Component
@StepScope
@RequiredArgsConstructor
public class FinalTargetProcessor implements ItemProcessor<StagingData, TargetItem> {

    private final List<TargetEnricher> enrichers;
    private final ObjectMapper objectMapper;

    @Value("#{jobParameters['domain']}")
    private String domain;

    @Override
    public TargetItem process(StagingData item) throws Exception {
        TargetItem target = new TargetItem();
        target.setRepresentativeContractId(item.getContractNo());
        target.setCustomerId(item.getCustomerNo());
        target.setDomainType(domain);

        for (TargetEnricher enricher : enrichers) {
            if (enricher.supports(domain)) {
                System.out.println("### [FinalTargetProcessor] Applying enricher: " + enricher.getClass().getSimpleName());
                enricher.enrich(target, item);
            }
        }

        target.setExtraInfo(objectMapper.writeValueAsString(target.getExtraInfoMap()));

        return target;
    }
}
