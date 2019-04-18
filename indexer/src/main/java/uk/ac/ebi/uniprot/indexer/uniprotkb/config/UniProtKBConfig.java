package uk.ac.ebi.uniprot.indexer.uniprotkb.config;

import net.jodah.failsafe.RetryPolicy;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.model.EntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.uniprotkb.processor.UniProtEntryProcessor;
import uk.ac.ebi.uniprot.indexer.uniprotkb.reader.UniProtEntryItemReader;
import uk.ac.ebi.uniprot.indexer.uniprotkb.writer.UniProtEntryDocumentPairWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

import java.io.File;
import java.time.temporal.ChronoUnit;

import static java.util.Collections.singletonList;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({ListenerConfig.class})
@EnableConfigurationProperties({UniProtKBIndexingProperties.class})
public class UniProtKBConfig {
    private final SolrTemplate solrTemplate;
    private UniProtKBIndexingProperties uniProtKBIndexingProperties = new UniProtKBIndexingProperties();

    public UniProtKBConfig(SolrTemplate solrTemplate) {
        this.solrTemplate = solrTemplate;
    }

    @Bean
    public ItemWriter<EntryDocumentPair<UniProtEntry, UniProtDocument>> uniProtDocumentItemWriter(RetryPolicy<Object> writeRetryPolicy) {
        return new UniProtEntryDocumentPairWriter(this.solrTemplate, SolrCollection.uniprot, writeRetryPolicy);
    }

    @Bean
    public RetryPolicy<Object> writeRetryPolicy() {
        return new RetryPolicy<>()
                .handle(singletonList(Exception.class))
                .withMaxRetries(uniProtKBIndexingProperties.getWriteRetryLimit())
                .withBackoff(uniProtKBIndexingProperties.getWriteRetryBackOffFromMillis(),
                             uniProtKBIndexingProperties.getWriteRetryBackOffToMillis(),
                             ChronoUnit.MILLIS);
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(new String[]{Constants.INDEX_FAILED_ENTRIES_COUNT_KEY,
                                                               Constants.INDEX_WRITTEN_ENTRIES_COUNT_KEY});
        return executionContextPromotionListener;
    }

    @Bean
    ItemReader<UniProtEntryDocumentPair> entryItemReader() {
        return new UniProtEntryItemReader(uniProtKBIndexingProperties);
    }

    @Bean
    ItemProcessor<UniProtEntryDocumentPair, UniProtEntryDocumentPair> uniProtDocumentItemProcessor() {
        return new UniProtEntryProcessor(createTaxonomyRepo(),
                                         createGoRelationRepo(),
                                         createKeywordRepo(),
                                         createPathwayRepo());
    }

    @Bean
    UniProtKBIndexingProperties indexingProperties() {
        return uniProtKBIndexingProperties;
    }

    private PathwayRepo createPathwayRepo() {
        return new PathwayFileRepo(uniProtKBIndexingProperties.getPathwayFile());
    }

    private KeywordRepo createKeywordRepo() {
        return new KeywordFileRepo(uniProtKBIndexingProperties.getKeywordFile());
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
