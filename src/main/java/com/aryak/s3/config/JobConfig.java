package com.aryak.s3.config;

public class JobConfig {

//    @Bean
//    @StepScope
//    public FlatFileItemReader<MyRecord> reader(
//            @Value("#{jobParameters['bucket']}") String bucket,
//            @Value("#{jobParameters['key']}") String key,
//            AmazonS3 amazonS3) {
//        S3Object s3Object = amazonS3.getObject(bucket, key);
//
//        return new FlatFileItemReaderBuilder<MyRecord>()
//                .name("s3FileReader")
//                .resource(new InputStreamResource(s3Object.getObjectContent()))
//                .delimited()
//                .names("col1", "col2", "col3")
//                .targetType(MyRecord.class)
//                .build();
//    }
}
