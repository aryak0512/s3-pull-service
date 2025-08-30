package com.aryak.s3.realtime;

import lombok.Data;

import java.time.LocalDateTime;

@Data
class FileStatus {

    private String jobId;
    private String filename;
    private long processedLines;
    private long totalLines;
    private double percentage;
    private String status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String errorMessage;
    private String estimatedTimeRemaining;

}
