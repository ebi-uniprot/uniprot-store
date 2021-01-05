package org.uniprot.store.indexer.publication.computational;

import java.io.IOException;
import java.util.List;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.publication.common.UniProtPublicationWriter;
import org.uniprot.store.search.document.publication.PublicationDocument;

@Configuration
public class ComputationalPublicationStep {
    private final StepBuilderFactory steps;
    private final UniProtSolrClient uniProtSolrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.computational.publication.file.path}"))
    private String filePath;

    @Autowired
    public ComputationalPublicationStep(
            StepBuilderFactory steps, UniProtSolrClient uniProtSolrClient) {
        this.steps = steps;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean(name = "IndexComputationalPublicationStep")
    public Step indexComputationalPublicationStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("computationallyMappedReferenceReader")
                    ItemReader<ComputationallyMappedReference> mappedReferenceReader,
            @Qualifier("computationallyMappedReferenceProcessor")
                    ItemProcessor<ComputationallyMappedReference, List<PublicationDocument>>
                            mappedReferenceProcessor,
            @Qualifier("computationallyMappedReferenceWriter")
                    ItemWriter<List<PublicationDocument>> mappedReferenceWriter) {
        return this.steps
                .get(Constants.COMPUTATIONAL_PUBLICATION_INDEX_STEP)
                .<ComputationallyMappedReference, List<PublicationDocument>>chunk(this.chunkSize)
                .reader(mappedReferenceReader)
                .processor(mappedReferenceProcessor)
                .writer(mappedReferenceWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "computationallyMappedReferenceReader")
    public ItemReader<ComputationallyMappedReference> xrefReader() throws IOException {
        return new ComputationalPublicationItemReader(this.filePath);
    }

    @Bean(name = "computationallyMappedReferenceProcessor")
    public ItemProcessor<ComputationallyMappedReference, List<PublicationDocument>>
            xrefProcessor() {
        return new ComputationalPublicationProcessor();
    }

    @Bean(name = "computationallyMappedReferenceWriter")
    public ItemWriter<List<PublicationDocument>> xrefWriter() {
        return new UniProtPublicationWriter(this.uniProtSolrClient);
    }
}
