package uk.ac.ebi.uniprot.indexer.uniprotkb.step;

import net.jodah.failsafe.RetryPolicy;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.cv.chebi.ChebiRepoFactory;
import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.common.listener.LogRateListener;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogStepListener;
import uk.ac.ebi.uniprot.indexer.common.model.EntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBConfig;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.uniprotkb.processor.UniProtEntryConverter;
import uk.ac.ebi.uniprot.indexer.uniprotkb.processor.UniProtEntryDocumentPairProcessor;
import uk.ac.ebi.uniprot.indexer.uniprotkb.reader.UniProtEntryItemReader;
import uk.ac.ebi.uniprot.indexer.uniprotkb.writer.UniProtEntryDocumentPairWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

import java.io.File;
import java.util.Map;

import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

/**
 * The main UniProtKB indexing step.
 * <p>
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({UniProtKBConfig.class, SuggestionStep.class})
public class UniProtKBStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtKBIndexingProperties uniProtKBIndexingProperties;
    private final SolrTemplate solrTemplate;

    @Autowired
    public UniProtKBStep(StepBuilderFactory stepBuilderFactory,
                         SolrTemplate solrTemplate,
                         UniProtKBIndexingProperties indexingProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.solrTemplate = solrTemplate;
        this.uniProtKBIndexingProperties = indexingProperties;
    }

    @Bean(name = "uniProtKBIndexingMainStep")
    public Step uniProtKBIndexingMainFFStep(WriteRetrierLogStepListener writeRetrierLogStepListener,
                                            @Qualifier("uniProtKB") LogRateListener<UniProtEntryDocumentPair> uniProtKBLogRateListener,
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
                .listener(uniProtKBLogRateListener)
                .build();
    }

    @Bean(name = "uniProtKB")
    public LogRateListener<UniProtEntryDocumentPair> uniProtKBLogRateListener() {
        return new LogRateListener<>(uniProtKBIndexingProperties.getUniProtKBLogRateInterval());
    }

    @Bean
    public ItemWriter<EntryDocumentPair<UniProtEntry, UniProtDocument>> uniProtDocumentItemWriter(RetryPolicy<Object> writeRetryPolicy) {
        return new UniProtEntryDocumentPairWriter(this.solrTemplate, SolrCollection.uniprot, writeRetryPolicy);
    }

    @Bean
    ItemReader<UniProtEntryDocumentPair> entryItemReader() {
        return new UniProtEntryItemReader(uniProtKBIndexingProperties);
    }

    @Bean
    ItemProcessor<UniProtEntryDocumentPair, UniProtEntryDocumentPair> uniProtDocumentItemProcessor(Map<String, SuggestDocument> suggestDocuments) {
        return new UniProtEntryDocumentPairProcessor(
                new UniProtEntryConverter(
                        createTaxonomyRepo(),
                        createGoRelationRepo(),
                        createPathwayRepo(),
                        ChebiRepoFactory.get(uniProtKBIndexingProperties.getChebiFile()),
                        suggestDocuments));
    }

    @Bean
    UniProtKBIndexingProperties indexingProperties() {
        return uniProtKBIndexingProperties;
    }

    private PathwayRepo createPathwayRepo() {
        return new PathwayFileRepo(uniProtKBIndexingProperties.getPathwayFile());
    }

    private GoRelationRepo createGoRelationRepo() {
        return new GoRelationFileRepo(
                new GoRelationFileReader(uniProtKBIndexingProperties.getGoDir()),
                new GoTermFileReader(uniProtKBIndexingProperties.getGoDir()));
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(uniProtKBIndexingProperties.getTaxonomyFile())));
    }
}