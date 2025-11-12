package com.nova.billing.preparation.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.test.context.ActiveProfiles;

@SpringBatchTest
@SpringBootTest
@ActiveProfiles("test") // 2. 이 테스트는 "test" 프로필로 실행
//@Sql(scripts = {"/schema.sql", "/data.sql"}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
public class PreparationJobConfigTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    public void setJob(Job preparationJob) {
        this.jobLauncherTestUtils.setJob(preparationJob);
    }

    @Test
    @Commit
    public void testWirelessPreparation() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("domain", "WIRELESS")
                .addString("billingDate", "2025-10-31")
                .addString("company", "SKT")
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    @Commit
    public void testWiredPreparation() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("domain", "WIRED")
                .addString("billingDate", "2025-10-31")
                .addString("company", "SKB")
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }
}
