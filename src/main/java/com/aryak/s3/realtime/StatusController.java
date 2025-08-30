package com.aryak.s3.realtime;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatusController {


    @GetMapping(value = "/live", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public void getFileCompletionPercentage() {

        // take filename from user

        String filename = "abc.txt";

        for ( int i = 1; i <= 10_000; i++ ) {
            FileStatus fileStatus = new FileStatus();
        }


    }

}
