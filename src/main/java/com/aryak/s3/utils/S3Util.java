package com.aryak.s3.utils;

import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@Component
public class S3Util {

    private final S3Client s3Client;

    public S3Util(S3Client s3Client) {
        this.s3Client = s3Client;
    }


    // This stream does not load the whole file — it pulls bytes on demand from S3.
    public ResponseInputStream<GetObjectResponse> s3InputStream(String bucket, String fileName) {
        return s3Client.getObject(getObjectRequest(bucket, fileName));
    }

    public GetObjectRequest getObjectRequest(String bucket, String fileName) {
        return GetObjectRequest.builder()
                .bucket(bucket)
                .key(fileName)
                .build();
    }

    public ResponseInputStream<GetObjectResponse> getResponseStream(String bucket, String key) {

        GetObjectRequest getObjectRequest = getObjectRequest(bucket, key);

        // Get only the raw InputStream of file content
        ResponseInputStream<GetObjectResponse> responseStream =
                s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());

        return responseStream;
    }
}
