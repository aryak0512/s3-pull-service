package com.aryak.s3.ftp;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPFile;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.filters.FtpSimplePatternFileListFilter;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;

import java.io.File;
import java.util.Arrays;

@Slf4j
@Configuration
public class FtpConfig {

    @Bean
    public DefaultFtpSessionFactory ftpSessionFactory() {
        DefaultFtpSessionFactory factory = new DefaultFtpSessionFactory();
        factory.setHost("localhost");
        factory.setPort(21);
        factory.setUsername("user");
        factory.setPassword("pass");
        return factory;
    }

    @Bean
    public IntegrationFlow ftpInboundFlow(DefaultFtpSessionFactory ftpSessionFactory) {
        return IntegrationFlow.from(
                        Ftp.inboundAdapter(ftpSessionFactory)
                                .preserveTimestamp(true)
                                .remoteDirectory("/")
                                .localDirectory(new File("ftp-files"))
                                .autoCreateLocalDirectory(true)
                                .deleteRemoteFiles(false)
                                .filter(new CompositeFileListFilter<>(Arrays.asList(
                                        new FtpSimplePatternFileListFilter("*"), // Accept all files
                                        (FileListFilter<FTPFile>) files -> {
                                            log.info("=== Files found in FTP directory ===");
                                            for ( FTPFile file : files ) {
                                                log.info("File: {} - Size: {} - Directory: {}",
                                                        file.getName(), file.getSize(), file.isDirectory());
                                            }
                                            log.info("=== End of files ===");
                                            return Arrays.asList(files);
                                        }
                                ))),


                        e -> e.poller(p -> p.fixedDelay(5000)
                                .errorHandler(t -> log.error("Polling error: ", t))) // Add error handling) // poll every 5s

                )

                .channel("ftpFileChannel") // required: where messages go
                .get();
    }

    @Bean
    public IntegrationFlow ftpFileConsumer() {
        return IntegrationFlow.from("ftpFileChannel")
                .handle(message -> {
                    File file = (File) message.getPayload();
                    System.out.println("New file: " + file.getAbsolutePath());
                })
                .get();
    }
}
