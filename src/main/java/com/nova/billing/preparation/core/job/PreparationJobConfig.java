package com.nova.billing.preparation.core.job;

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
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.nova.billing.preparation.core.model.PreparationParameter;
import com.nova.billing.preparation.core.model.TargetItem;
import com.nova.billing.preparation.core.spi.PreparationManager;
import com.nova.billing.preparation.core.spi.PreparationManagerFactory;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class PreparationJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final PreparationManagerFactory managerFactory;
    private final DataSource dataSource;

    public static final String JOB_NAME = "PreparationJob";

    @Bean(name = JOB_NAME)
    public Job preparationJob() {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(preparationStep())
                .build();
    }

    @Bean
    @JobScope
    public Step preparationStep() {
        return new StepBuilder("preparationStep", jobRepository)
                .<Object, TargetItem>chunk(100, transactionManager)
                .reader(preparationReader(null, null, null))
                .processor(preparationProcessor(null, null, null))
                .writer(preparationWriter())
                .build();
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
