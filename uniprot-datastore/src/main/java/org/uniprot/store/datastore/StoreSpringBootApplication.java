package org.uniprot.store.datastore;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.uniprot.store.datastore.app.UniProtStoreJob;

@EnableBatchProcessing
@UniProtStoreJob
@SpringBootApplication
public class StoreSpringBootApplication {
    /**
     * To run a specific Spring Batch Job, ensure you have the correct properties in
     * application.properties configured correctly. There MUST be a property called:
     * uniprot.job.name=
     *
     * <pre>
     * java -jar SPRING_BOOT_APP_JAR -Dspring.batch.job.names=JOB_NAMES
     * </pre>
     *
     * Note: the Spring Boot indexing application should return an appropriate exit code upon
     * application success / failure. This is critical for any host environment that runs the
     * application, which itself handles process status codes. For example, using LSF to dispatch
     * Spring Boot applications, it is important to return the success or failure of the application
     * to LSF, so that we receive accurate LSF notifications by email.
     *
     * @param args arguments to the application, typically none, since properties files will be
     *     used.
     * @return the status code of the application's execution
     */
    static <T> int run(Class<T> type, String[] args) {
        ConfigurableApplicationContext applicationContext = SpringApplication.run(type, args);
        return SpringApplication.exit(applicationContext);
    }

    public static void main(String[] args) {
        System.exit(StoreSpringBootApplication.run(StoreSpringBootApplication.class, args));
    }
}
