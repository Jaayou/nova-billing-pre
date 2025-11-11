package com.nova.billing.preparation.core.job;

import java.math.BigDecimal;
import java.time.LocalDate;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import com.nova.billing.preparation.core.model.PreparationParameter;
import com.nova.billing.preparation.core.model.StagingData;
import com.nova.billing.preparation.core.model.TargetItem;
import com.nova.billing.preparation.core.processor.FinalTargetProcessor;
import com.nova.billing.preparation.core.spi.PreparationManager;
import com.nova.billing.preparation.core.spi.PreparationManagerFactory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class PreparationJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final PreparationManagerFactory managerFactory;
    private final DataSource dataSource;
    private final FinalTargetProcessor finalTargetProcessor;
    private final ItemWriter<TargetItem> preparationWriter;

    public static final String JOB_NAME = "PreparationJob";

    @Bean(name = JOB_NAME)
    public Job preparationJob() {
        return new JobBuilder(JOB_NAME, jobRepository)
                // .start(preparationStep())
                .start(extractActiveContractsStep())
                .next(summarizeUsageStep())
                .next(finalJoinAndEnrichStep())
                .build();
    }

    // @Bean
    // @JobScope
    // public Step preparationStep() {
    // return new StepBuilder("preparationStep", jobRepository)
    // .<Object, TargetItem>chunk(100, transactionManager)
    // .reader(preparationReader(null, null, null))
    // .processor(preparationProcessor(null, null, null))
    // .writer(preparationWriter())
    // .build();
    // }

    @Bean
    public Step extractActiveContractsStep() {
        return new StepBuilder("extractActiveContractsStep", jobRepository)
                .<ContractInfo, ContractInfo>chunk(100, transactionManager)
                .reader(step1Reader())
                .writer(step1Writer())
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<ContractInfo> step1Reader() {
        // [Step 1 Reader] 계약 테이블과 고객 정보 테이블을 조인
        SqlPagingQueryProviderFactoryBean providerFactory = new SqlPagingQueryProviderFactoryBean();
        providerFactory.setDataSource(dataSource);
        providerFactory.setSelectClause("SELECT T1.CONTRACT_NO, T1.CUSTOMER_NO, T2.GRADE, T1.ACTIVATION_DATE");
        providerFactory.setFromClause("FROM REQ_WIRELESS_CONTRACT T1 JOIN CUSTOMER_INFO T2 ON T1.CUSTOMER_NO = T2.CUSTOMER_NO");
        providerFactory.setWhereClause("WHERE T1.STATUS = 'ACTIVE'");
        //providerFactory.setSortKey("T1.CONTRACT_NO");
        providerFactory.setSortKey("CONTRACT_NO");

        return new JdbcPagingItemReaderBuilder<ContractInfo>()
                .name("step1Reader")
                .dataSource(dataSource)
                .pageSize(1000)
                .queryProvider(getQueryProvider(providerFactory))
                .rowMapper(new BeanPropertyRowMapper<>(ContractInfo.class))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<ContractInfo> step1Writer() {
        // [Step 1 Writer] Staging 1 테이블에 저장
        return new JdbcBatchItemWriterBuilder<ContractInfo>()
                .dataSource(dataSource)
                .sql("INSERT INTO STAGING_CONTRACTS (CONTRACT_NO, CUSTOMER_NO, GRADE, ACTIVATION_DATE) " +
                     "VALUES (:contractNo, :customerNo, :grade, :activationDate)")
                .beanMapped()
                .build();
    }

    @Bean
    public Step summarizeUsageStep() {
        return new StepBuilder("summarizeUsageStep", jobRepository)
                .<UsageSummary, UsageSummary>chunk(1000, transactionManager)
                .reader(step2Reader())
                .writer(step2Writer())
                .build();
    }

@Bean
    @StepScope
    public ItemReader<UsageSummary> step2Reader() {
        // [Step 2 Reader] 원시 사용량 데이터를 계약별로 집계(GROUP BY)
        SqlPagingQueryProviderFactoryBean providerFactory = new SqlPagingQueryProviderFactoryBean();
        providerFactory.setDataSource(dataSource);
        providerFactory.setSelectClause("CONTRACT_NO, SUM(USAGE_AMOUNT) AS TOTAL_USAGE_AMOUNT");
        providerFactory.setFromClause("FROM USAGE_RAW_DATA");
        providerFactory.setGroupClause("GROUP BY CONTRACT_NO"); // [핵심] 집계
        providerFactory.setSortKey("CONTRACT_NO");

        return new JdbcPagingItemReaderBuilder<UsageSummary>()
                .name("step2Reader")
                .dataSource(dataSource)
                .pageSize(1000)
                .queryProvider(getQueryProvider(providerFactory))
                .rowMapper(new BeanPropertyRowMapper<>(UsageSummary.class))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<UsageSummary> step2Writer() {
        // [Step 2 Writer] Staging 2 테이블에 저장
        return new JdbcBatchItemWriterBuilder<UsageSummary>()
                .dataSource(dataSource)
                .sql("INSERT INTO STAGING_USAGE (CONTRACT_NO, TOTAL_USAGE_AMOUNT) " +
                     "VALUES (:contractNo, :totalUsageAmount)")
                .beanMapped()
                .build();
    }

    @Bean
    @JobScope
    public Step finalJoinAndEnrichStep() {
        return new StepBuilder("finalJoinAndEnrichStep", jobRepository)
                .<StagingData, TargetItem>chunk(100, transactionManager)
                .reader(step3Reader())
                .processor(finalTargetProcessor)
                .writer(step3Writer())
                .build();
    }

@Bean
    @StepScope
    public ItemReader<StagingData> step3Reader() {
        // [Step 3 Reader] 두 개의 임시 테이블(Staging)을 조인
        SqlPagingQueryProviderFactoryBean providerFactory = new SqlPagingQueryProviderFactoryBean();
        providerFactory.setDataSource(dataSource);
        providerFactory.setSelectClause("SELECT T1.CONTRACT_NO, T1.CUSTOMER_NO, T1.GRADE, T1.ACTIVATION_DATE, T2.TOTAL_USAGE_AMOUNT");
        providerFactory.setFromClause("FROM STAGING_CONTRACTS T1 LEFT OUTER JOIN STAGING_USAGE T2 ON T1.CONTRACT_NO = T2.CONTRACT_NO");
        //providerFactory.setSortKey("T1.CONTRACT_NO");
        providerFactory.setSortKey("CONTRACT_NO");

        return new JdbcPagingItemReaderBuilder<StagingData>()
                .name("step3Reader")
                .dataSource(dataSource)
                .pageSize(1000)
                .queryProvider(getQueryProvider(providerFactory))
                .rowMapper(new BeanPropertyRowMapper<>(StagingData.class))
                .build();
    }

    @Bean
    @StepScope
    public ItemWriter<TargetItem> step3Writer() {
        // [Step 3 Writer] v0.02에서 만든 최종 Writer 재사용
        return new JdbcBatchItemWriterBuilder<TargetItem>()
                .dataSource(dataSource)
                .sql("INSERT INTO BAT_PREPARATION_TARGET (REP_CONTRACT_ID, CUSTOMER_ID, DOMAIN_TYPE, EXTRA_INFO) " +
                     "VALUES (:representativeContractId, :customerId, :domainType, :extraInfo)")
                .beanMapped()
                .build();
    }

// --- Helper DTOs & Methods ---

    // PagingQueryProvider 생성 헬퍼
    private PagingQueryProvider getQueryProvider(SqlPagingQueryProviderFactoryBean factory) {
        try {
            return factory.getObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create query provider", e);
        }
    }

    // Step 1 Reader/Writer DTO
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class ContractInfo {
        private Long contractNo;
        private Long customerNo;
        private String grade;
        private LocalDate activationDate;
    }

    // Step 2 Reader/Writer DTO
    @Data @NoArgsConstructor @AllArgsConstructor
    public static class UsageSummary {
        private Long contractNo;
        private BigDecimal totalUsageAmount;
    }    

    @Bean
    @StepScope
    public ItemReader<Object> preparationReader(
            @Value("#{jobParameters['domain']}") String domain,
            @Value("#{jobParameters['billingDate']}") String billingDateStr,
            @Value("#{jobParameters['company']}") String company) {

        PreparationParameter param = buildParam(domain, billingDateStr, company);
        PreparationManager manager = managerFactory.findManager(domain);

        System.out.println("[PreparationJob] Selected Manager for domain: " + domain);
        return manager.createReader(param);
    }

    @Bean
    @StepScope
    public ItemReader<StagingData> finalJoinReader() {
        SqlPagingQueryProviderFactoryBean providerFactory = new SqlPagingQueryProviderFactoryBean();
        providerFactory.setDataSource(dataSource);
        providerFactory.setSelectClause(
                "SELECT T1.CONTRACT_NO, T1.CUSTOMER_NO, T1.GRADE, T1.ACTIVATION_DATE, T2.TOTAL_USAGE_AMOUNT");
        providerFactory.setFromClause(
                "FROM STAGING_CONTRACTS T1 LEFT OUTER JOIN STAGING_USAGE T2 ON T1.CONTRACT_NO = T2.CONTRACT_NO");
        //providerFactory.setSortKey("T1.CONTRACT_NO");
        providerFactory.setSortKey("CONTRACT_NO");

        PagingQueryProvider queryProvider;
        try {
            queryProvider = providerFactory.getObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create final join query provider", e);
        }

        return new JdbcPagingItemReaderBuilder<StagingData>()
                .name("finalJoinReader")
                .dataSource(dataSource)
                .pageSize(100)
                .queryProvider(queryProvider)
                .rowMapper(new BeanPropertyRowMapper<>(StagingData.class))
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<Object, TargetItem> preparationProcessor(
            @Value("#{jobParameters['domain']}") String domain,
            @Value("#{jobParameters['billingDate']}") String billingDateStr,
            @Value("#{jobParameters['company']}") String company) {

        PreparationParameter param = buildParam(domain, billingDateStr, company);
        PreparationManager manager = managerFactory.findManager(domain);

        return manager.createProcessor(param);
    }

    @Bean
    @StepScope
    public ItemWriter<TargetItem> preparationWriter() {
        // return items -> {
        // System.out.println(" [CommonWriter] Writing " + items.size() + " items to
        // Target Table...");
        // for (TargetItem item : items) {
        // System.out.println(" -> Inserted: " + item);
        // }
        // };

        return new JdbcBatchItemWriterBuilder<TargetItem>()
                .dataSource(dataSource)
                .sql("INSERT INTO BAT_PREPARATION_TARGET (REP_CONTRACT_ID, CUSTOMER_ID, DOMAIN_TYPE, EXTRA_INFO) " +
                        "VALUES (:representativeContractId, :customerId, :domainType, :extraInfo)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }

    private PreparationParameter buildParam(String domain, String dateStr, String company) {
        return PreparationParameter.builder()
                .domain(domain)
                .billingDate(dateStr != null ? LocalDate.parse(dateStr) : LocalDate.now())
                .company(company)
                .build();
    }
}
