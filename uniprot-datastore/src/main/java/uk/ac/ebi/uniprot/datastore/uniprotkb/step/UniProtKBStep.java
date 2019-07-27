package uk.ac.ebi.uniprot.datastore.uniprotkb.step;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.datastore.UniProtStoreClient;
import uk.ac.ebi.uniprot.datastore.common.config.StoreConfig;
import uk.ac.ebi.uniprot.datastore.common.listener.LogRateListener;
import uk.ac.ebi.uniprot.datastore.common.listener.WriteRetrierLogStepListener;
import uk.ac.ebi.uniprot.datastore.uniprotkb.config.AsyncConfig;
import uk.ac.ebi.uniprot.datastore.uniprotkb.config.UniProtKBConfig;
import uk.ac.ebi.uniprot.datastore.uniprotkb.config.UniProtKBStoreProperties;
import uk.ac.ebi.uniprot.datastore.uniprotkb.reader.UniProtEntryItemReader;
import uk.ac.ebi.uniprot.datastore.uniprotkb.writer.UniProtEntryRetryWriter;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;

import java.io.File;

import static uk.ac.ebi.uniprot.datastore.utils.Constants.UNIPROTKB_STORE_STEP;


/**
 * The main UniProtKB indexing step.
 * <p>
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({UniProtKBConfig.class, StoreConfig.class, AsyncConfig.class})
@Slf4j
public class UniProtKBStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtKBStoreProperties uniProtKBStoreProperties;

    @Autowired
    public UniProtKBStep(StepBuilderFactory stepBuilderFactory,
                         UniProtKBStoreProperties uniProtKBStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniProtKBStoreProperties = uniProtKBStoreProperties;
    }

    @Bean(name = "uniProtKBDataStoreMainStep")
    public Step uniProtKBIndexingMainFFStep(WriteRetrierLogStepListener writeRetrierLogStepListener,
                                            @Qualifier("uniProtKB") LogRateListener<UniProtEntry> uniProtKBLogRateListener,
                                            ItemReader<UniProtEntry> entryItemReader,
                                            ItemProcessor<UniProtEntry, UniProtEntry> uniProtEntryPassThroughProcessor,
                                            UniProtEntryRetryWriter uniProtEntryWriter,
                                            ExecutionContextPromotionListener promotionListener) throws Exception {

        return this.stepBuilderFactory.get(UNIPROTKB_STORE_STEP)
                .listener(promotionListener)
                .<UniProtEntry, UniProtEntry>chunk(uniProtKBStoreProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(uniProtEntryPassThroughProcessor)
                .writer(uniProtEntryWriter)
                .listener(writeRetrierLogStepListener)
                .listener(uniProtKBLogRateListener)
                .listener(unwrapProxy(uniProtEntryWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<UniProtEntry> entryItemReader() {
        return new UniProtEntryItemReader(uniProtKBStoreProperties);
    }

    // ---------------------- Processors ----------------------
    @Bean
    public ItemProcessor<UniProtEntry, UniProtEntry> uniProtEntryPassThroughProcessor() {
        return new PassThroughItemProcessor<>();
    }

    // ---------------------- Writers ----------------------
    @Bean
    public UniProtEntryRetryWriter uniProtDocumentItemWriter(UniProtStoreClient<UniProtEntry> uniProtStoreClient,
                                                             RetryPolicy<Object> writeRetryPolicy) {
        return new UniProtEntryRetryWriter(entries -> entries
                .forEach(uniProtStoreClient::saveEntry), writeRetryPolicy);
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "uniProtKB")
    public LogRateListener<UniProtEntry> uniProtKBLogRateListener() {
        return new LogRateListener<>(uniProtKBStoreProperties.getUniProtKBLogRateInterval());
    }

    // ---------------------- Source Data Access beans and helpers ----------------------
    /**
     * Needs to be a bean since it contains a @Cacheable annotation within, and Spring
     * will only scan for these annotations inside beans.
     *
     * @return the GoRelationFileRepo
     */
    @Bean
    public GoRelationFileRepo goRelationFileRepo() {
        return new GoRelationFileRepo(
                new GoRelationFileReader(uniProtKBStoreProperties.getGoDir()),
                new GoTermFileReader(uniProtKBStoreProperties.getGoDir()));
    }

    private PathwayRepo createPathwayRepo() {
        return new PathwayFileRepo(uniProtKBStoreProperties.getPathwayFile());
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(uniProtKBStoreProperties.getTaxonomyFile())));
    }

    /**
     * Checks if the given object is a proxy, and unwraps it if it is.
     *
     * @param bean The object to check
     * @return The unwrapped object that was proxied, else the object
     * @throws Exception any exception caused during unwrapping
     */
    private Object unwrapProxy(Object bean) throws Exception {
        if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
            Advised advised = (Advised) bean;
            bean = advised.getTargetSource().getTarget();
        }
        return bean;
    }
}