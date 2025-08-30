package com.aryak.s3.realtime;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDateTime;

@RestController
public class TestController {

    @CrossOrigin("*")
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ProcessEvent> updateLinesProcessed() {

        return Flux.range(1, 1000)
                .map(p -> new ProcessEvent("filename.csv", p, LocalDateTime.now()))
                .delayElements(Duration.ofMillis(200));

    }
}
