package com.nova.billing.preparation.core.model;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;
import lombok.ToString;

@Data
public class TargetItem {
    private Long representativeContractId;
    private Long customerId;
    private String domainType;
    private String billingCycle;

    @ToString.Exclude
    private Map<String, Object> extraInfoMap = new HashMap<>();

    private String extraInfo;

    public void addExtraInfo(String key, Object value) {
        this.extraInfoMap.put(key, value);
    }
}
