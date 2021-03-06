package org.uniprot.store.indexer.proteome;

import static org.uniprot.store.indexer.common.utils.Constants.SUGGESTIONS_INDEX_STEP;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.uniprotkb.reader.SuggestionItemReader;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author sahmad
 * @created 24-08-2020
 */
@Configuration
@Import({ProteomeConfig.class})
public class ProteomeSuggestionStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtSolrClient uniProtSolrClient;

    @Value(("${solr.indexing.chunkSize}"))
    private int chunkSize = 100;

    @Autowired
    public ProteomeSuggestionStep(
            StepBuilderFactory stepBuilderFactory, UniProtSolrClient uniProtSolrClient) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean(name = "suggestionProteomeIndexingStep")
    public Step proteomeSuggestionStep(
            StepExecutionListener stepListener,
            SuggestionItemReader suggestionItemReader,
            ExecutionContextPromotionListener promotionListener,
            @Qualifier("suggestionProteome")
                    LogRateListener<SuggestDocument> suggestionLogRateListener) {
        return this.stepBuilderFactory
                .get(SUGGESTIONS_INDEX_STEP)
                .listener(promotionListener)
                .<SuggestDocument, SuggestDocument>chunk(chunkSize)
                .reader(suggestionItemReader)
                .writer(new SolrDocumentWriter<>(uniProtSolrClient, SolrCollection.suggest))
                .listener(stepListener)
                .listener(suggestionLogRateListener)
                .build();
    }

    @Bean(name = "suggestionProteome")
    public LogRateListener<SuggestDocument> suggestionLogRateListener() {
        return new LogRateListener<>();
    }

    @Bean
    public SuggestionItemReader suggestionItemReader() {
        return new SuggestionItemReader();
    }
}
