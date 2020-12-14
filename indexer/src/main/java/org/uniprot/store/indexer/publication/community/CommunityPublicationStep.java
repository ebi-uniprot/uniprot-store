package org.uniprot.store.indexer.publication.community;

import java.io.IOException;

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
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

@Configuration
public class CommunityPublicationStep {
    private final StepBuilderFactory steps;
    private final UniProtSolrClient uniProtSolrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.community.publication.file.path}"))
    private String filePath;

    @Autowired
    public CommunityPublicationStep(StepBuilderFactory steps, UniProtSolrClient uniProtSolrClient) {
        this.steps = steps;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean(name = "IndexCommunityPublicationStep")
    public Step indexCommunityPublicationStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("mappedReferenceReader")
                    ItemReader<CommunityMappedReference> mappedReferenceReader,
            @Qualifier("mappedReferenceProcessor")
                    ItemProcessor<CommunityMappedReference, PublicationDocument>
                            mappedReferenceProcessor,
            @Qualifier("mappedReferenceWriter")
                    ItemWriter<PublicationDocument> mappedReferenceWriter) {
        return this.steps
                .get(Constants.COMMUNITY_PUBLICATION_INDEX_STEP)
                .<CommunityMappedReference, PublicationDocument>chunk(this.chunkSize)
                .reader(mappedReferenceReader)
                .processor(mappedReferenceProcessor)
                .writer(mappedReferenceWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "mappedReferenceReader")
    public ItemReader<CommunityMappedReference> xrefReader() throws IOException {
        return new CommunityPublicationItemReader(this.filePath);
    }

    @Bean(name = "mappedReferenceProcessor")
    public ItemProcessor<CommunityMappedReference, PublicationDocument> xrefProcessor() {
        return new CommunityPublicationProcessor();
    }

    @Bean(name = "mappedReferenceWriter")
    public ItemWriter<PublicationDocument> xrefWriter() {
        return new SolrDocumentWriter<>(this.uniProtSolrClient, SolrCollection.publication);
    }
}
