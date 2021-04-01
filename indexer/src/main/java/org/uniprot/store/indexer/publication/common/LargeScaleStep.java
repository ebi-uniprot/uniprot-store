package org.uniprot.store.indexer.publication.common;

import java.util.Set;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * @author lgonzales
 * @since 13/01/2021
 */
@Configuration
public class LargeScaleStep {

    private final StepBuilderFactory steps;
    private final UniProtSolrClient uniProtSolrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    private LargeScaleSolrFieldQuery solrFieldName;

    @Autowired
    public LargeScaleStep(
            StepBuilderFactory steps,
            UniProtSolrClient uniProtSolrClient,
            LargeScaleSolrFieldQuery solrFieldName) {
        this.steps = steps;
        this.uniProtSolrClient = uniProtSolrClient;
        this.solrFieldName = solrFieldName;
    }

    @Bean(name = "cacheLargeScaleStep")
    public Step cacheLargeScaleStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("largeScaleReader") ItemReader<Set<String>> largeScaleReader,
            @Qualifier("largeScaleWriter") ItemWriter<Set<String>> largeScaleWriter) {
        return this.steps
                .get(Constants.PUBLICATION_LARGE_SCALE_STEP)
                .<Set<String>, Set<String>>chunk(this.chunkSize)
                .reader(largeScaleReader)
                .writer(largeScaleWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "largeScaleReader")
    public ItemReader<Set<String>> largeScaleReader() {
        return new LargeScaleReader(this.uniProtSolrClient, solrFieldName);
    }

    @Bean(name = "largeScaleWriter")
    public ItemWriter<Set<String>> largeScaleWriter() {
        return new LargeScaleWriter();
    }
}
