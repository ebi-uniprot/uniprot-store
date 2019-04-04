package uk.ac.ebi.uniprot.indexer.common.listeners;

import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
/**
 *
 * @author lgonzales
 */
@Configuration
public class ListenerConfig {

    @Bean
    public JobExecutionListener jobListener() {
        return new LogJobListener();

    }

    @Bean
    public StepExecutionListener stepListener(){
        return new LogStepListener();
    }
}
