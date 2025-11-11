package com.nova.billing.preparation.core.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@Profile("!test") // 2. "test" 프로필이 아닐 때만 실행
@RequiredArgsConstructor
public class JobRunner implements CommandLineRunner {

    private final JobLauncher jobLauncher;
    private final Job preparationJob;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("### [JobRunner] Application started. Running PreparationJob...");

        JobParameters jobParameters = new JobParametersBuilder()
        .addString("domain", "WIRELESS")
        .addString("billingDate", "2025-10-31")
        .addString("company", "SKT")
        .addLong("timestamp", System.currentTimeMillis())
        .toJobParameters();

        jobLauncher.run(preparationJob, jobParameters);

        System.out.println("### [JobRunner] PreparationJob COMPLETED.");
    }
}
