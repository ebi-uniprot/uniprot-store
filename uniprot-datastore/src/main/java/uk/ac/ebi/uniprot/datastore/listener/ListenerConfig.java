package uk.ac.ebi.uniprot.datastore.listener;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author lgonzales
 */
@Configuration
public class ListenerConfig {
    @Bean
    public LogRateListener writeRateListener() {
        return new LogRateListener();
    }

    @Bean
    public WriteRetrierLogJobListener writeRetrierLogJobListener() {
        return new WriteRetrierLogJobListener();
    }

    @Bean
    public WriteRetrierLogStepListener writeRetrierLogStepListener() {
        return new WriteRetrierLogStepListener();
    }
}
