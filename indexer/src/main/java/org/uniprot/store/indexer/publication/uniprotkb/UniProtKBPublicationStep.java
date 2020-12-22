package org.uniprot.store.indexer.publication.uniprotkb;

import java.util.List;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.publication.common.UniProtPublicationWriter;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBConfig;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.indexer.uniprotkb.reader.UniProtEntryItemReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

@Configuration
@Import({UniProtKBConfig.class})
public class UniProtKBPublicationStep {
    private final StepBuilderFactory steps;
    private final UniProtSolrClient uniProtSolrClient;
    private final UniProtKBIndexingProperties uniProtKBIndexingProperties;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Autowired
    public UniProtKBPublicationStep(
            StepBuilderFactory steps,
            UniProtSolrClient uniProtSolrClient,
            UniProtKBIndexingProperties indexingProperties) {
        this.steps = steps;
        this.uniProtSolrClient = uniProtSolrClient;
        this.uniProtKBIndexingProperties = indexingProperties;
    }

    @Bean(name = "indexUniProtKBPublicationStep")
    public Step indexUniProtKBPublicationStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<UniProtEntryDocumentPair> uniProtEntryItemReader,
            ItemProcessor<UniProtEntryDocumentPair, List<PublicationDocument>>
                    uniProtPublicationProcessor,
            ItemWriter<List<PublicationDocument>> uniProtPublicationWriter) {
        return this.steps
                .get(Constants.UNIPROTKB_PUBLICATION_INDEX_STEP)
                .<UniProtEntryDocumentPair, List<PublicationDocument>>chunk(this.chunkSize)
                .reader(uniProtEntryItemReader)
                .processor(uniProtPublicationProcessor)
                .writer(uniProtPublicationWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean
    public ItemReader<UniProtEntryDocumentPair> uniProtEntryItemReader() {
        return new UniProtEntryItemReader(uniProtKBIndexingProperties);
    }

    @Bean
    public ItemProcessor<UniProtEntryDocumentPair, List<PublicationDocument>>
            uniProtPublicationProcessor() {
        return new UniProtPublicationProcessor(this.uniProtSolrClient, SolrCollection.publication);
    }

    @Bean
    public ItemWriter<List<PublicationDocument>> uniProtPublicationWriter() {
        return new UniProtPublicationWriter(this.uniProtSolrClient);
    }
}
