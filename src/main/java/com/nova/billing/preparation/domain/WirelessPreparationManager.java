package com.nova.billing.preparation.domain;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.stereotype.Component;

import com.nova.billing.preparation.core.model.PreparationParameter;
import com.nova.billing.preparation.core.model.TargetItem;
import com.nova.billing.preparation.core.spi.PreparationManager;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class WirelessPreparationManager implements PreparationManager {

    private final DataSource dataSource;

    @Override
    public boolean supports(String domain) {
        return "WIRELESS".equalsIgnoreCase(domain);
    }

    @Override
    public ItemReader<Object> createReader(PreparationParameter param) {
        System.out.println("  [WirelessManager] Creating JDBC Reader for ACTIVE contracts...");

        return new JdbcPagingItemReaderBuilder<Object>()
                .name("wirelessContractReader")
                .dataSource(dataSource)
                .pageSize(100)
                .rowMapper((rs, rowNum) -> new WirelessContract(
                        rs.getLong("CONTRACT_NO"),
                        rs.getLong("CUSTOMER_NO"),
                        rs.getString("PLAN_CODE")))
                .queryProvider(createQueryProvider())
                .build();

        // List<Object> items = new ArrayList<>();
        // items.add(new WirelessContract(1001L, 5001L, "5G_STD"));
        // items.add(new WirelessContract(1002L, 5002L, "LTE_BASIC"));

        // return new ListItemReader<>(items);
    }

    private PagingQueryProvider createQueryProvider() {
        try {
            //H2PagingQueryProvider provider = new H2PagingQueryProvider();
            SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
            factory.setDataSource(dataSource);
            factory.setSelectClause("SELECT CONTRACT_NO, CUSTOMER_NO, PLAN_CODE");
            factory.setFromClause("FROM REQ_WIRELESS_CONTRACT");
            factory.setWhereClause("WHERE STATUS = 'ACTIVE'");
            factory.setSortKey("CONTRACT_NO");
            return factory.getObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create query provider", e);
        }
    }

    @Override
    public ItemProcessor<Object, TargetItem> createProcessor(PreparationParameter param) {
        return item -> {
            WirelessContract contract = (WirelessContract) item;
            TargetItem target = new TargetItem();
            target.setRepresentativeContractId(contract.getContractId());
            target.setCustomerId(contract.getCustomerId());
            target.setDomainType("WIRELESS");

            Map<String, Object> info = new HashMap<>();
            info.put("networkType", contract.planCode.startsWith("5G") ? "5G" : "LTE");
            target.setExtraInfo(info.toString());

            return target;
        };
    }

    // for Test
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class WirelessContract {
        Long contractId;
        Long customerId;
        String planCode;
    }
}
