package com.nova.billing.preparation.core.spi;

import java.util.List;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class PreparationManagerFactory {
    private final List<PreparationManager> managers;

    public PreparationManager findManager(String domain) {
        return managers.stream()
                .filter(manager -> manager.supports(domain))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No PreparationManager found for domain: " + domain));
    }
}
