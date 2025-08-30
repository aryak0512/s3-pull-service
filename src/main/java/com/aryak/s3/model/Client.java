package com.aryak.s3.model;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class Client {

    private Integer id;
    private String name;
    private String columns;
    private boolean header;
    private boolean footer;
    private int linesToSkip = 1;
    private boolean enabled;
    private String delimiter = ",";
    private FileType fileType;

    // more fields to be added later as needed

    public String[] getColumnNames() {
        return this.columns.split(",");
    }
}
