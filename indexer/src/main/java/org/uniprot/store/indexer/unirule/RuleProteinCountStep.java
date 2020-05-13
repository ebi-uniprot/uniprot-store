package org.uniprot.store.indexer.unirule;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** @author sahmad */
@Configuration
public class RuleProteinCountStep {
    @Value(("${database.chunk.size}"))
    private Integer chunkSize;

    @Bean
    public Step proteinCountStep(
            StepBuilderFactory stepBuilders,
            StepExecutionListener stepListener,
            ChunkListener chunkListener) {
        return null;
    }
}
