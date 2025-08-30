package com.aryak.s3.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotification;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotificationRecord;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class S3FileListener {

    private static final Logger log = LoggerFactory.getLogger(S3FileListener.class);

    private final JobLauncher jobLauncher;
    //private final Job job;

    private final Set<String> processedFiles = new HashSet<>();

    public S3FileListener(JobLauncher jobLauncher) {
        this.jobLauncher = jobLauncher;
        //this.job = job;
    }

    //@SqsListener("${sqs.queue-name}")
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
                boolean duplicate = checkIfDuplicate(key);

                log.info("Duplicate : {}", duplicate);

                // identify client from directory and get its metadata


                if ( !duplicate ) {

                    log.info("New file detected in S3 bucket : {}, Key : {}", bucket, key);
                    JobParameters parameters = new JobParametersBuilder()
                            .addJobParameter("bucket", bucket, String.class)
                            .addJobParameter("key", key, String.class)
                            //.addJobParameter("timestamp", System.currentTimeMillis(), Long.class)
                            .toJobParameters();

                    //jobLauncher.run(job, parameters);
                }

            }

        } catch (Exception e) {
            log.error("Unexpected error while processing S3 event: {}", messageJson, e);
        }

    }

    // DB check to be connected later
    private boolean checkIfDuplicate(String key) {
        return processedFiles.contains(key);
    }
}
