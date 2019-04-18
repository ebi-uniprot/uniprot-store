package uk.ac.ebi.uniprot.indexer.common.listeners;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogJobListener;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogRateListener;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogStepListener;

/**
 * @author lgonzales
 */
@Configuration
public class ListenerConfig {
    @Bean
    public JobExecutionListener jobListener() {
        return new LogJobListener();
    }

    @Bean
    public StepExecutionListener stepListener() {
        return new LogStepListener();
    }

    @Bean
    public ChunkListener chunkListener() {
        return new LogChunkListener();
    }

    @Bean
    public WriteRetrierLogJobListener writeRetrierLogJobListener() {
        return new WriteRetrierLogJobListener();
    }

    @Bean
    public WriteRetrierLogRateListener writeRetrierLogRateListener() {
        return new WriteRetrierLogRateListener();
    }

    @Bean
    public WriteRetrierLogStepListener writeRetrierLogStepListener() {
        return new WriteRetrierLogStepListener();
    }
}
