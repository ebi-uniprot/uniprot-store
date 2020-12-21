package org.uniprot.store.indexer.publication.computational;

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
import org.uniprot.store.indexer.publication.common.PublicationWriter;
import org.uniprot.store.search.document.publication.PublicationDocument;

import java.io.IOException;

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
                    ItemProcessor<ComputationallyMappedReference, PublicationDocument>
                            mappedReferenceProcessor,
            @Qualifier("computationallyMappedReferenceWriter")
                    ItemWriter<PublicationDocument> mappedReferenceWriter) {
        return this.steps
                .get(Constants.COMPUTATIONAL_PUBLICATION_INDEX_STEP)
                .<ComputationallyMappedReference, PublicationDocument>chunk(this.chunkSize)
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
    public ItemProcessor<ComputationallyMappedReference, PublicationDocument> xrefProcessor() {
        return new ComputationalPublicationProcessor(this.uniProtSolrClient);
    }

    @Bean(name = "computationallyMappedReferenceWriter")
    public ItemWriter<PublicationDocument> xrefWriter() {
        return new PublicationWriter(this.uniProtSolrClient);
    }
}
