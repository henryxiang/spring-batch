package com.example.demo.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Date;

@Configuration
@EnableBatchProcessing
public class TestJob {
    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step stepTest() {
        return stepBuilderFactory.get("Test Step")
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println("**** Test Job: Step 1 ****");
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Job jobTest() {
//        String jobName = "Job" + new Date().toString();
        String jobName = "Job 1";
        return jobBuilderFactory.get(jobName)
                .incrementer(new RunIdIncrementer())
                .start(stepTest()).build();
    }
}
