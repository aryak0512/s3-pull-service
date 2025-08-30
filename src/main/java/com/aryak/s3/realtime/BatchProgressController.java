package com.aryak.s3.realtime;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.time.Duration;

@RestController
@RequestMapping("/api/batch")
public class BatchProgressController {

    private final BatchProgressService batchProgressService;

    public BatchProgressController(BatchProgressService batchProgressService) {
        this.batchProgressService = batchProgressService;
    }

    @GetMapping(value = "/live/{jobId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<FileStatus> getJobProgress(@PathVariable String jobId) {
        return batchProgressService.getProgressStream(jobId)
                .delayElements(Duration.ofMillis(500)) // Throttle updates to avoid overwhelming client
                .doOnCancel(() -> System.out.println("Client disconnected from job: " + jobId))
                .doOnComplete(() -> System.out.println("Job completed: " + jobId));
    }

    @PostMapping("/start")
    public ResponseEntity<JobStartResponse> startBatchJob(@RequestBody JobStartRequest request) {
        String jobId = batchProgressService.startJob(request.getFilename(), request.getTotalLines());
        return ResponseEntity.ok(new JobStartResponse(jobId, "Job started successfully"));
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<FileStatus> getJobStatus(@PathVariable String jobId) {
        FileStatus status = batchProgressService.getCurrentStatus(jobId);
        if ( status != null ) {
            return ResponseEntity.ok(status);
        }
        return ResponseEntity.notFound().build();
    }
}