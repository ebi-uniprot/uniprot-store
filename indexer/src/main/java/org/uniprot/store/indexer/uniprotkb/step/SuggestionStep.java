package org.uniprot.store.indexer.uniprotkb.step;

import static org.uniprot.store.indexer.common.utils.Constants.SUGGESTIONS_INDEX_STEP;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.indexer.uniprotkb.config.SuggestionConfig;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBConfig;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import org.uniprot.store.indexer.uniprotkb.reader.SuggestionItemReader;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.LogStepListener;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * Created 15/05/19
 *
 * @author Edd
 */
@Configuration
@Import({SuggestionConfig.class, UniProtKBConfig.class})
public class SuggestionStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtKBIndexingProperties indexingProperties;
    private final UniProtSolrOperations solrOperations;

    @Autowired
    public SuggestionStep(
            StepBuilderFactory stepBuilderFactory,
            UniProtKBIndexingProperties indexingProperties,
            UniProtSolrOperations solrOperations) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.indexingProperties = indexingProperties;
        this.solrOperations = solrOperations;
    }

    @Bean(name = "suggestionIndexingStep")
    public Step suggestionStep(
            SuggestionItemReader suggestionItemReader,
            ExecutionContextPromotionListener promotionListener,
            @Qualifier("suggestion") LogRateListener<SuggestDocument> suggestionLogRateListener) {
        return this.stepBuilderFactory
                .get(SUGGESTIONS_INDEX_STEP)
                .listener(promotionListener)
                .<SuggestDocument, SuggestDocument>chunk(indexingProperties.getChunkSize())
                .reader(suggestionItemReader)
                .writer(new SolrDocumentWriter<>(solrOperations, SolrCollection.suggest))
                .listener(new LogStepListener())
                .listener(suggestionLogRateListener)
                .build();
    }

    @Bean(name = "suggestion")
    public LogRateListener<SuggestDocument> suggestionLogRateListener() {
        return new LogRateListener<>(indexingProperties.getSuggestionLogRateInterval());
    }

    @Bean
    public SuggestionItemReader suggestionItemReader() {
        return new SuggestionItemReader();
    }
}
