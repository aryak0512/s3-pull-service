package com.aryak.s3.config;

import com.aryak.s3.reader.FileReaderFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

@Slf4j
//@Configuration
public class JobConfig {

    @Bean
    public Job job(JobRepository jobRepository,
                   Step step2
                   //@Value("#{jobParameters['key']}") String key
    ) {
        return new JobBuilder("nightly-job", jobRepository)
                .start(step2)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository,
                     PlatformTransactionManager transactionManager,
                     FileReaderFactory fileReaderFactory) {
        return new StepBuilder("step", jobRepository)
                .<Map<String, String>, Map<String, String>>chunk(100, transactionManager)
                .reader(fileReaderFactory.getReader(null, null))
                //.taskExecutor(taskExecutor())
                .taskExecutor(taskExecutor())
                .processor(item -> {
                    log.info("Item : {}", item);
                    return item;
                })
                .writer(it -> System.out.println("Read approach 2 completed"))
                .build();
    }


    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("batch-");
        executor.initialize();
        return executor;
    }

}
