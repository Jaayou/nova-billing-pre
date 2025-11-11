package com.nova.billing.preparation.domain.wireless.enricher;

import java.time.LocalDate;
import java.time.Period;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.nova.billing.preparation.core.model.StagingData;
import com.nova.billing.preparation.core.model.TargetItem;
import com.nova.billing.preparation.core.spi.TargetEnricher;

@Component
@StepScope
public class LongTermFlagEnricher implements TargetEnricher {

    @Value("#{jobParameters['billingDate']}")
    private String billingDateStr;

    @Override
    public boolean supports(String domain) {
        return "WIRELESS".equals(domain);
    }

    @Override
    public void enrich(TargetItem target, StagingData rawData) {
        LocalDate billingDate = LocalDate.parse(billingDateStr);
        LocalDate activationDate = rawData.getActivationDate();

        if (activationDate == null) {
            target.addExtraInfo("isLongTerm", false);
            return;
        }

        Period period = Period.between(activationDate, billingDate);

        if (period.getYears() >= 3) {
            target.addExtraInfo("isLongTerm", true);
        } else {
            target.addExtraInfo("isLongTerm", false);
        }
    }
}
