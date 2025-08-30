package com.aryak.s3.realtime;

import lombok.Data;

@Data
class JobStartRequest {
    
    private String filename;
    private long totalLines;

}
