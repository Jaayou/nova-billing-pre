package com.nova.billing.preparation.domain.wired;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
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
public class WiredPreparationManager implements PreparationManager {

    private final DataSource dataSource;

    @Override
    public boolean supports(String domain) {
        return "WIRED".equalsIgnoreCase(domain);
    }

    @Override
    public ItemReader<Object> createReader(PreparationParameter param) {
        System.out.println("  [WiredManager] Creating JDBC Reader for WIRED contracts...");

        SqlPagingQueryProviderFactoryBean providerFactory = new SqlPagingQueryProviderFactoryBean();
        providerFactory.setDataSource(dataSource);
        providerFactory.setSelectClause("SELECT CONTRACT_NO, CUSTOMER_NO, SERVICE_TYPE");
        providerFactory.setFromClause("FROM REQ_WIRED_CONTRACT");
        providerFactory.setWhereClause("WHERE STATUS = 'ACTIVE'");
        providerFactory.setSortKey("CONTRACT_NO");

        return new JdbcPagingItemReaderBuilder<Object>()
                .name("wiredContractReader")
                .dataSource(dataSource)
                .pageSize(100)
                .queryProvider(getQueryProvider(providerFactory))
                .rowMapper((rs, rowNum) -> new BeanPropertyRowMapper<>(WiredContract.class).mapRow(rs, rowNum))
                .build();
    }

    @Override
    public ItemProcessor<Object, TargetItem> createProcessor(PreparationParameter param) {
        return item -> {
            WiredContract contract = (WiredContract) item;
            TargetItem target = new TargetItem();
            target.setRepresentativeContractId(contract.getContractNo());
            target.setCustomerId(contract.getCustomerNo());
            target.setDomainType("WIRED");

            Map<String, Object> info = new HashMap<>();
            info.put("serviceType", contract.getServiceType());
            target.setExtraInfo(info.toString());

            return target;
        };
    }

    private PagingQueryProvider getQueryProvider(SqlPagingQueryProviderFactoryBean factory) {
        try {
            return factory.getObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create query provider", e);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class WiredContract {
        private Long contractNo;
        private Long customerNo;
        private String serviceType;
    }
}
