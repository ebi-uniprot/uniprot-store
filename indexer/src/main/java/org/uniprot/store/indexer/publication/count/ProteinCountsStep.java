package org.uniprot.store.indexer.publication.count;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.publication.common.UniProtPublicationWriter;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBConfig;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * @author sahmad
 * @created 23/12/2020
 */
@Configuration
@Import({UniProtKBConfig.class})
public class ProteinCountsStep {
    private final StepBuilderFactory steps;
    private final UniProtSolrClient solrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    public ProteinCountsStep(StepBuilderFactory steps, UniProtSolrClient solrClient) {
        this.steps = steps;
        this.solrClient = solrClient;
    }

    @Bean(name = "indexPublicationStatsStep")
    public Step indexPublicationStatsStep(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            ItemReader<LiteratureDocument> literatureReader,
            ItemProcessor<LiteratureDocument, List<PublicationDocument>> compositeItemProcessor,
            ItemWriter<List<PublicationDocument>> publicationWriter) {
        return this.steps
                .get(Constants.PUBLICATIONS_STATS_INDEX_STEP)
                .<LiteratureDocument, List<PublicationDocument>>chunk(this.chunkSize)
                .reader(literatureReader)
                .processor(compositeItemProcessor)
                .writer(publicationWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean
    public ItemReader<LiteratureDocument> literatureDocumentReader() {
        return new LiteratureDocumentReader((solrClient));
    }

    @Bean
    public ItemProcessor<LiteratureDocument, List<PublicationDocument>> compositeItemProcessor() {
        CompositeItemProcessor<LiteratureDocument, List<PublicationDocument>> compositeProcessor =
                new CompositeItemProcessor<>();
        List itemProcessors = new ArrayList();
        itemProcessors.add(new TypesFacetProcessor(solrClient));
        itemProcessors.add(new DocumentsStatsProcessor(solrClient));
        compositeProcessor.setDelegates(itemProcessors);
        return compositeProcessor;
    }

    @Bean
    public ItemWriter<List<PublicationDocument>> publicationWriter() {
        return new UniProtPublicationWriter(this.solrClient);
    }
}
