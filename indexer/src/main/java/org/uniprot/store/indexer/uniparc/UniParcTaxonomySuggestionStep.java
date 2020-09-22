package org.uniprot.store.indexer.uniparc;

import static org.uniprot.store.indexer.common.utils.Constants.SUGGESTIONS_INDEX_STEP;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.uniprotkb.reader.SuggestionItemReader;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.LogStepListener;
import org.uniprot.store.job.common.util.CommonConstants;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author sahmad
 * @created 25/08/2020
 */
@Configuration
public class UniParcTaxonomySuggestionStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtSolrClient uniProtSolrClient;

    @Value(("${uniparc.indexing.chunkSize}"))
    private int chunkSize = 1000;

    @Autowired
    public UniParcTaxonomySuggestionStep(
            StepBuilderFactory stepBuilderFactory, UniProtSolrClient uniProtSolrClient) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean(name = "uniparcTaxonomySuggestionStep")
    public Step uniparcTaxonomySuggestionStep(
            @Qualifier("uniParcSuggestionReader") SuggestionItemReader suggestionItemReader,
            @Qualifier("uniParcSuggestionListener")
                    ExecutionContextPromotionListener promotionListener,
            @Qualifier("uniParcSuggestion")
                    LogRateListener<SuggestDocument> suggestionLogRateListener) {
        return this.stepBuilderFactory
                .get(SUGGESTIONS_INDEX_STEP)
                .listener(promotionListener)
                .<SuggestDocument, SuggestDocument>chunk(chunkSize)
                .reader(suggestionItemReader)
                .writer(new SolrDocumentWriter<>(uniProtSolrClient, SolrCollection.suggest))
                .listener(new LogStepListener())
                .listener(suggestionLogRateListener)
                .build();
    }

    @Bean(name = "uniParcSuggestion")
    public LogRateListener<SuggestDocument> suggestionLogRateListener() {
        return new LogRateListener<>();
    }

    @Bean(name = "uniParcSuggestionReader")
    public SuggestionItemReader suggestionItemReader() {
        return new SuggestionItemReader();
    }

    @Bean("uniParcSuggestionListener")
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener =
                new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(
                new String[] {
                    CommonConstants.FAILED_ENTRIES_COUNT_KEY,
                    CommonConstants.WRITTEN_ENTRIES_COUNT_KEY,
                    Constants.SUGGESTIONS_MAP
                });
        return executionContextPromotionListener;
    }
}
