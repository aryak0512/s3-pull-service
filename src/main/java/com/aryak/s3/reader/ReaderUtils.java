package com.aryak.s3.reader;

import com.aryak.s3.model.InputRecord;
import com.aryak.s3.model.MyRecord;
import com.aryak.s3.utils.S3Util;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

@Slf4j
@Configuration
public class ReaderUtils {

    private final S3Util s3Util;

    public ReaderUtils(S3Util s3Util) {
        this.s3Util = s3Util;
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
        var responseStream = s3Util.getResponseStream(bucket, key);

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
        try ( ResponseInputStream<GetObjectResponse> s3Stream = s3Util.getResponseStream(bucket, key) ) {
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


}
