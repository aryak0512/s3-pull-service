package com.aryak.s3.reader;

import com.aryak.s3.model.Client;
import com.aryak.s3.model.FileType;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Component
public class FileReaderFactory {

    public ItemReader<Map<String, String>> getReader(File file, Client metadata) {
        if ( FileType.CSV == metadata.getFileType() ) {
            return csvReader(file, metadata);
        }
        throw new IllegalArgumentException("Unsupported file type: " + metadata.getFileType());
    }

    private FlatFileItemReader<Map<String, String>> csvReader(File file, Client metadata) {
        return new FlatFileItemReaderBuilder<Map<String, String>>()
                .name("csv-reader")
                .resource(new FileSystemResource(file))
                .delimited()
                .delimiter(metadata.getDelimiter())
                .names(metadata.getColumnNames())
                .fieldSetMapper(fieldSet -> {

                    // need to check what fieldSet contains

                    Map<String, String> row = new HashMap<>();
                    for ( String col : metadata.getColumnNames() ) {
                        row.put(col, fieldSet.readString(col));
                    }
                    return row;
                })
                .build();
    }

}
