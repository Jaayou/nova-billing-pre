package com.nova.billing.preparation.core.model;

import lombok.Data;

@Data
public class TargetItem {
    private Long representativeContractId;
    private Long customerId;
    private String domainType;
    private String billingCycle;

    private String extraInfo;
}
