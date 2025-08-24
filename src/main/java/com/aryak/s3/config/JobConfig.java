package com.aryak.s3.config;

import com.aryak.s3.model.MyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;

@Configuration
public class JobConfig {

    private static final Logger log = LoggerFactory.getLogger(JobConfig.class);
    private final S3Client s3Client;

    public JobConfig(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Bean
    public Job firstJob(JobRepository jobRepository, Step step1) {
        return new JobBuilder("job1", jobRepository)
                .start(step1)
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      FlatFileItemReader<MyRecord> reader) {
        return new StepBuilder("step1", jobRepository)
                .<MyRecord, MyRecord>chunk(100, transactionManager)
                .reader(reader)
                .writer(it -> System.out.println("hi"))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<MyRecord> reader(
            @Value("#{jobParameters['bucket']}") String bucket,
            @Value("#{jobParameters['key']}") String key) throws IOException {

        log.info("Reader Bucket : {} and key : {}", bucket, key);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        // Get only the raw InputStream of file content
        ResponseInputStream<GetObjectResponse> responseStream =
                s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());

        // To make it restartable, copy into memory (or temp file)
        byte[] fileBytes = responseStream.readAllBytes();

        return new FlatFileItemReaderBuilder<MyRecord>()
                .name("s3FileReader")
                .resource(new org.springframework.core.io.ByteArrayResource(fileBytes))
                .delimited()
                .names("id", "name", "city")
                .targetType(MyRecord.class)
                .build();
    }


}
