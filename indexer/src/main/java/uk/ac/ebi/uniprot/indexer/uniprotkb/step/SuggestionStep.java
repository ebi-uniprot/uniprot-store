package uk.ac.ebi.uniprot.indexer.uniprotkb.step;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.listener.LogRateListener;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.SuggestionConfig;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBConfig;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import uk.ac.ebi.uniprot.indexer.uniprotkb.reader.SuggestionItemReader;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;

import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.SUGGESTIONS_INDEX_STEP;

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
    private final SolrTemplate solrTemplate;

    @Autowired
    public SuggestionStep(StepBuilderFactory stepBuilderFactory,
                          UniProtKBIndexingProperties indexingProperties,
                          SolrTemplate solrTemplate) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.indexingProperties = indexingProperties;
        this.solrTemplate = solrTemplate;
    }

    @Bean(name = "yyyy")
    public Step suggestionStep(SuggestionItemReader suggestionItemReader,
                               ExecutionContextPromotionListener promotionListener) {
        return this.stepBuilderFactory.get(SUGGESTIONS_INDEX_STEP)
                .listener(promotionListener)
                .<SuggestDocument, SuggestDocument>chunk(indexingProperties.getChunkSize())
                .reader(suggestionItemReader)
                .writer(new SolrDocumentWriter<>(solrTemplate, SolrCollection.suggest))
                .listener(new LogRateListener<>())
                .build();
    }

    @Bean
    SuggestionItemReader suggestionItemReader() {
        return new SuggestionItemReader();
    }
}
