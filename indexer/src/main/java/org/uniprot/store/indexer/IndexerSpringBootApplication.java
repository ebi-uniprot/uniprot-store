/*
 * Created by sahmad on 28/01/19 19:14
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package org.uniprot.store.indexer;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.uniprot.store.indexer.app.UniProtIndexingJob;

@EnableBatchProcessing
@UniProtIndexingJob
@SpringBootApplication
public class IndexerSpringBootApplication {
    /**
     * To run a specific Spring Batch Job:
     * <pre>
     * java -jar SPRING_BOOT_APP_JAR -Dspring.batch.job.names=JOB_NAMES
     * </pre>
     * Note: the Spring Boot indexing application should return an appropriate exit code upon application success / failure.
     * This is critical for any host environment that runs the application, which itself handles process status codes.
     * For example, using LSF to dispatch Spring Boot applications, it is important to return the success or failure
     * of the application to LSF, so that we receive accurate LSF notifications by email.
     *
     * @param args arguments to the application, typically none, since properties files will be used.
     * @return the status code of the application's execution
     */
    static <T> int run(Class<T> type, String[] args) {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(type, args);
        return SpringApplication.exit(applicationContext);
    }

    public static void main(String[] args) {
        System.exit(IndexerSpringBootApplication.run(IndexerSpringBootApplication.class, args));
    }
}


