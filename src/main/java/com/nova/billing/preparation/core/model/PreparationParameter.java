package com.nova.billing.preparation.core.model;

import java.time.LocalDate;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class PreparationParameter {
    private String company; //T, B, ...
    private String domain; //WIRELESS, WIRED,...
    private LocalDate billingDate; //INV_DT
    private String jobType; //MAIN, ESTIMATE, SIMULATION,...
    private String jobCycle; // 01, 02, ...
}
