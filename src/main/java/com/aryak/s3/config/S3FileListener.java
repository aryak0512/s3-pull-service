package com.aryak.s3.config;

import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotification;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotificationRecord;

import java.util.List;

@Component
public class S3FileListener {

    private static final Logger log = LoggerFactory.getLogger(S3FileListener.class);

    @SqsListener("${sqs.queue-name}")
    public void handleS3Event(String messageJson) {
        try {
            // Try parsing the incoming JSON as S3 event
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
                // TODO: trigger your Spring Batch job here
            }

        } catch (Exception e) {
            // Catch-all safeguard
            log.error("Unexpected error while processing S3 event: {}", messageJson, e);
        }

    }
}
