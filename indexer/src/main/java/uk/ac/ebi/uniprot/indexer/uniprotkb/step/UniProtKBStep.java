package uk.ac.ebi.uniprot.indexer.uniprotkb.step;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.common.listener.LogRateListener;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogStepListener;
import uk.ac.ebi.uniprot.indexer.common.model.EntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBConfig;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

/**
 * The main UniProtKB indexing step.
 *
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({UniProtKBConfig.class})
public class UniProtKBStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtKBIndexingProperties uniProtKBIndexingProperties;

    @Autowired
    public UniProtKBStep(StepBuilderFactory stepBuilderFactory,
                         UniProtKBIndexingProperties indexingProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniProtKBIndexingProperties = indexingProperties;
    }

    @Bean("UniProtKBIndexStep")
    public Step uniProtKBIndexingMainFFStep(WriteRetrierLogStepListener writeRetrierLogStepListener,
                                            ItemReader<UniProtEntryDocumentPair> entryItemReader,
                                            ItemProcessor<UniProtEntryDocumentPair, UniProtEntryDocumentPair> uniProtDocumentItemProcessor,
                                            ItemWriter<EntryDocumentPair<UniProtEntry, UniProtDocument>> uniProtDocumentItemWriter,
                                            ExecutionContextPromotionListener promotionListener) {
        return this.stepBuilderFactory.get(UNIPROTKB_INDEX_STEP)
                .listener(promotionListener)
                .<UniProtEntryDocumentPair, UniProtEntryDocumentPair>chunk(uniProtKBIndexingProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(uniProtDocumentItemProcessor)
                .writer(uniProtDocumentItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(new LogRateListener<UniProtEntryDocumentPair>())
                .build();
    }
}