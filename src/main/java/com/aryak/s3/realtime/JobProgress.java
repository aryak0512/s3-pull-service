package com.aryak.s3.realtime;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

class JobProgress {

    @Getter
    private final String jobId;
    
    @Getter
    private final String filename;

    @Getter
    private final long totalLines;

    private final AtomicLong processedLines = new AtomicLong(0);

    @Getter
    private final LocalDateTime startTime;

    @Setter
    @Getter
    private volatile LocalDateTime endTime;

    @Setter
    @Getter
    private volatile String status = "RUNNING";

    @Setter
    @Getter
    private volatile String errorMessage;

    public JobProgress(String jobId, String filename, long totalLines) {
        this.jobId = jobId;
        this.filename = filename;
        this.totalLines = totalLines;
        this.startTime = LocalDateTime.now();
    }

    public void incrementProcessedLines(long increment) {
        processedLines.addAndGet(increment);
    }

    public boolean isCompleted() {
        return processedLines.get() >= totalLines;
    }

    public double getPercentage() {
        return totalLines > 0 ? (double) processedLines.get() / totalLines * 100 : 0;
    }

    public long getProcessedLines() {
        return processedLines.get();
    }

}
