package com.aryak.s3.listeners;

import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotification;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotificationRecord;

import java.util.List;

@Component
public class S3FileListener {

    private static final Logger log = LoggerFactory.getLogger(S3FileListener.class);
    @Autowired
    JobLauncher jobLauncher;
    @Autowired
    Job firstJob;

    @SqsListener("${sqs.queue-name}")
    public void handleS3Event(String messageJson) {
        try {

            S3EventNotification event = S3EventNotification.fromJson(messageJson);
            List<S3EventNotificationRecord> records = event.getRecords();

            if ( records == null || records.isEmpty() ) {
                log.warn("S3 Event with no records: {}", messageJson);
                return; // don't crash, just skip
            }

            for ( S3EventNotificationRecord record : records ) {

                String bucket = record.getS3().getBucket().getName();
                String key = record.getS3().getObject().getKey();

                log.info("New S3 file detected. Bucket: {}, Key: {}", bucket, key);

                JobParameters parameters = new JobParametersBuilder()
                        .addJobParameter("bucket", bucket, String.class)
                        .addJobParameter("key", key, String.class)
                        .toJobParameters();

                jobLauncher.run(firstJob, parameters);
            }

        } catch (Exception e) {
            log.error("Unexpected error while processing S3 event: {}", messageJson, e);
        }

    }
}
