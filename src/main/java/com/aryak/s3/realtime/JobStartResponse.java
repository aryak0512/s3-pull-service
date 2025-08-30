package com.aryak.s3.realtime;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
class JobStartResponse {
    
    private String jobId;
    private String message;

}
