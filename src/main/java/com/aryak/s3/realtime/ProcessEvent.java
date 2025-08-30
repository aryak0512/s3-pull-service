package com.aryak.s3.realtime;

import java.time.LocalDateTime;

public record ProcessEvent(

        String filename,
        int linesProcessed,
        LocalDateTime currentTime
) {
}
