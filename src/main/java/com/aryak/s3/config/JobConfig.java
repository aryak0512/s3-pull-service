package com.aryak.s3.config;

import com.aryak.s3.model.InputRecord;
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
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

@Configuration
public class JobConfig {

    private static final Logger log = LoggerFactory.getLogger(JobConfig.class);
    private final S3Client s3Client;

    public JobConfig(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    //@Bean
    public Job firstJob(JobRepository jobRepository, Step step1) {
        return new JobBuilder("job1", jobRepository)
                .start(step1)
                .build();
    }

    @Bean
    public Job secondJob(JobRepository jobRepository, Step step2) {
        return new JobBuilder("job2", jobRepository)
                .start(step2)
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      FlatFileItemReader<MyRecord> reader) {
        return new StepBuilder("step1", jobRepository)
                .<MyRecord, MyRecord>chunk(100, transactionManager)
                .reader(reader)
                .writer(it -> System.out.println("Read approach 1 completed"))
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager,
                      FlatFileItemReader<InputRecord> reader2) {
        return new StepBuilder("step2", jobRepository)
                .<InputRecord, InputRecord>chunk(100, transactionManager)
                .reader(reader2)
                .taskExecutor(taskExecutor())
                .processor(item -> {
                    log.info("Item : {}", item);
                    return item;
                })
                .writer(it -> System.out.println("Read approach 2 completed"))
                .build();
    }

    /**
     * Approach 1 : Loads entire file into main memory
     *
     * @param bucket
     * @param key
     * @return
     * @throws IOException
     */
    @Bean
    @StepScope
    public FlatFileItemReader<MyRecord> reader(
            @Value("#{jobParameters['bucket']}") String bucket,
            @Value("#{jobParameters['key']}") String key) throws IOException {

        log.info("Reader Bucket : {} and key : {}", bucket, key);

        GetObjectRequest getObjectRequest = getObjectRequest(bucket, key);

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

    public GetObjectRequest getObjectRequest(String bucket, String fileName) {
        return GetObjectRequest.builder()
                .bucket(bucket)
                .key(fileName)
                .build();
    }

    // This stream does not load the whole file — it pulls bytes on demand from S3.
    public ResponseInputStream<GetObjectResponse> s3InputStream(String bucket, String fileName) {
        return s3Client.getObject(getObjectRequest(bucket, fileName));
    }


    /**
     * Approach 2 : Not loading the entire file into main memory
     * <b>THIS CODE IS BUGGY & ERROR PRONE</b>
     *
     * @param bucket
     * @param key
     * @return
     * @throws IOException
     */
    //@Bean
    @StepScope
    public FlatFileItemReader<InputRecord> reader12(
            @Value("#{jobParameters['bucket']}") String bucket,
            @Value("#{jobParameters['key']}") String key) throws IOException {

        ResponseInputStream<GetObjectResponse> s3Stream = s3InputStream(bucket, key);

        InputStream bufferedStream = new BufferedInputStream(s3Stream);

        return new FlatFileItemReaderBuilder<InputRecord>()
                .name("inputRecordReader")
                .resource(new InputStreamResource(bufferedStream))
                .delimited()
                .names("id", "first_name", "last_name", "email", "gender", "ip_address")
                .targetType(InputRecord.class)
                .build();
    }

    /**
     * Approach 3 : Downloads to a temp file on disk and then reads (fast)
     * This temp file is created at <code>System.getProperty("java.io.tmpdir")</code>
     * and will not be auto deleted
     * <p>
     * For MacOS it was : <code>/var/folders/v9/4lmftyjs2r35qk843p9t8p080000gn/T/</code>
     *
     * @param bucket
     * @param key
     * @return
     * @throws IOException
     */
    @Bean
    @StepScope
    public FlatFileItemReader<InputRecord> reader2(
            @Value("#{jobParameters['bucket']}") String bucket,
            @Value("#{jobParameters['key']}") String key) throws IOException {

        File tempFile = File.createTempFile("s3-", ".csv");
        try ( ResponseInputStream<GetObjectResponse> s3Stream = s3InputStream(bucket, key) ) {
            Files.copy(s3Stream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        return new FlatFileItemReaderBuilder<InputRecord>()
                .name("inputRecordReader")
                .resource(new FileSystemResource(tempFile))
                .delimited()
                .names("id", "first_name", "last_name", "email", "gender", "ip_address")
                .targetType(InputRecord.class)
                .build();

        // some housekeeping cleanup later
        // finally {
        //    if (tempFile != null && tempFile.exists()) {
        //        tempFile.delete();
        //    }
        //}
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
