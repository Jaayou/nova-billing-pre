package com.nova.billing.preparation.core.model;

import java.math.BigDecimal;
import java.time.LocalDate;

import lombok.Data;

@Data
public class StagingData {
    private Long contractNo;
    private Long customerNo;
    private String grade;
    private LocalDate activationDate;
    private BigDecimal totalUsageAmount;
}
