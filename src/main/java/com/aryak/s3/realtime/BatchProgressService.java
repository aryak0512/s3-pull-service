package com.aryak.s3.realtime;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class BatchProgressService {

    private final Map<String, Sinks.Many<FileStatus>> progressSinks = new ConcurrentHashMap<>();
    private final Map<String, JobProgress> jobProgressMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

    public String startJob(String filename, long totalLines) {
        String jobId = UUID.randomUUID().toString();

        // Create sink for this job
        Sinks.Many<FileStatus> sink = Sinks.many().multicast().onBackpressureBuffer();
        progressSinks.put(jobId, sink);

        // Initialize job progress
        JobProgress jobProgress = new JobProgress(jobId, filename, totalLines);
        jobProgressMap.put(jobId, jobProgress);

        // Start the batch job simulation
        startBatchJobSimulation(jobId, filename, totalLines);

        return jobId;
    }

    public Flux<FileStatus> getProgressStream(String jobId) {
        Sinks.Many<FileStatus> sink = progressSinks.get(jobId);
        if ( sink == null ) {
            return Flux.error(new RuntimeException("Job not found: " + jobId));
        }

        // Send current status immediately if available
        JobProgress progress = jobProgressMap.get(jobId);
        if ( progress != null ) {
            FileStatus currentStatus = createFileStatus(progress);
            sink.tryEmitNext(currentStatus);
        }

        return sink.asFlux()
                .doOnCancel(() -> cleanupJob(jobId))
                .onErrorResume(throwable -> {
                    System.err.println("Error in progress stream for job " + jobId + ": " + throwable.getMessage());
                    return Flux.empty();
                });
    }

    public FileStatus getCurrentStatus(String jobId) {
        JobProgress progress = jobProgressMap.get(jobId);
        return progress != null ? createFileStatus(progress) : null;
    }

    private void startBatchJobSimulation(String jobId, String filename, long totalLines) {
        JobProgress progress = jobProgressMap.get(jobId);
        Sinks.Many<FileStatus> sink = progressSinks.get(jobId);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if ( progress.isCompleted() ) {
                    return;
                }

                // Simulate processing lines (random increment between 10-100 lines per update)
                long increment = 10 + (long) (Math.random() * 90);
                progress.incrementProcessedLines(Math.min(increment,
                        progress.getTotalLines() - progress.getProcessedLines()));

                FileStatus status = createFileStatus(progress);

                // Emit progress update
                Sinks.EmitResult result = sink.tryEmitNext(status);
                if ( result.isFailure() ) {
                    System.err.println("Failed to emit progress for job " + jobId + ": " + result);
                }

                // Complete the job when done
                if ( progress.isCompleted() ) {
                    progress.setStatus("COMPLETED");
                    progress.setEndTime(LocalDateTime.now());

                    FileStatus finalStatus = createFileStatus(progress);
                    sink.tryEmitNext(finalStatus);
                    sink.tryEmitComplete();

                    // Clean up after a delay
                    scheduler.schedule(() -> cleanupJob(jobId), 30, TimeUnit.SECONDS);
                }

            } catch (Exception e) {
                System.err.println("Error processing job " + jobId + ": " + e.getMessage());
                progress.setStatus("FAILED");
                progress.setErrorMessage(e.getMessage());

                FileStatus errorStatus = createFileStatus(progress);
                sink.tryEmitNext(errorStatus);
                sink.tryEmitError(e);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private FileStatus createFileStatus(JobProgress progress) {
        FileStatus status = new FileStatus();
        status.setJobId(progress.getJobId());
        status.setFilename(progress.getFilename());
        status.setProcessedLines(progress.getProcessedLines());
        status.setTotalLines(progress.getTotalLines());
        status.setPercentage(progress.getPercentage());
        status.setStatus(progress.getStatus());
        status.setStartTime(progress.getStartTime());
        status.setEndTime(progress.getEndTime());
        status.setErrorMessage(progress.getErrorMessage());
        status.setEstimatedTimeRemaining(calculateEstimatedTime(progress));
        return status;
    }

    private String calculateEstimatedTime(JobProgress progress) {
        if ( progress.isCompleted() || progress.getProcessedLines() == 0 ) {
            return null;
        }

        long elapsed = Duration.between(progress.getStartTime(), LocalDateTime.now()).toSeconds();
        long remaining = progress.getTotalLines() - progress.getProcessedLines();
        long avgLinesPerSecond = progress.getProcessedLines() / Math.max(elapsed, 1);

        if ( avgLinesPerSecond > 0 ) {
            long estimatedSeconds = remaining / avgLinesPerSecond;
            return formatDuration(estimatedSeconds);
        }

        return "Calculating...";
    }

    private String formatDuration(long seconds) {
        if ( seconds < 60 ) return seconds + "s";
        if ( seconds < 3600 ) return (seconds / 60) + "m " + (seconds % 60) + "s";
        return (seconds / 3600) + "h " + ((seconds % 3600) / 60) + "m";
    }

    private void cleanupJob(String jobId) {
        progressSinks.remove(jobId);
        jobProgressMap.remove(jobId);
        System.out.println("Cleaned up job: " + jobId);
    }
}
