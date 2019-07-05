package uk.ac.ebi.uniprot.indexer.uniprotkb.step;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import uk.ac.ebi.uniprot.cv.chebi.ChebiRepoFactory;
import uk.ac.ebi.uniprot.cv.ec.ECRepoFactory;
import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.indexer.common.concurrency.TaskExecutorProperties;
import uk.ac.ebi.uniprot.indexer.common.listener.LogRateListener;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogStepListener;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
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

import java.io.File;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

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
@Slf4j
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
                                            UniProtEntryDocumentPairProcessor uniProtDocumentItemProcessor,
                                            UniProtEntryDocumentPairWriter uniProtDocumentItemWriter,
                                            ExecutionContextPromotionListener promotionListener) {
        return this.stepBuilderFactory.get(UNIPROTKB_INDEX_STEP)
                .listener(promotionListener)
                .<UniProtEntryDocumentPair, Future<UniProtEntryDocumentPair>>
                        chunk(uniProtKBIndexingProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(asyncProcessor(uniProtDocumentItemProcessor))
                .writer(asyncWriter(uniProtDocumentItemWriter))
                .listener(writeRetrierLogStepListener)
                .listener(uniProtKBLogRateListener)
                .listener(uniProtDocumentItemProcessor)
                .listener(uniProtDocumentItemWriter)
                .build();
    }

    @Bean(name = "uniProtKB")
    public LogRateListener<UniProtEntryDocumentPair> uniProtKBLogRateListener() {
        return new LogRateListener<>(uniProtKBIndexingProperties.getUniProtKBLogRateInterval());
    }

    @Bean
    @StepScope
    public UniProtEntryDocumentPairWriter uniProtDocumentItemWriter(RetryPolicy<Object> writeRetryPolicy) {
        return new UniProtEntryDocumentPairWriter(this.solrTemplate, SolrCollection.uniprot, writeRetryPolicy);
    }

    /**
     * Needs to be a bean since it contains a @Cacheable annotation within, and Spring
     * will only scan for these annotations inside beans.
     * @return the GoRelationFileRepo
     */
    @Bean
    @StepScope
    public GoRelationFileRepo goRelationFileRepo() {
        return new GoRelationFileRepo(
                new GoRelationFileReader(uniProtKBIndexingProperties.getGoDir()),
                new GoTermFileReader(uniProtKBIndexingProperties.getGoDir()));
    }

    @Bean
    @StepScope
    UniProtEntryDocumentPairProcessor uniProtDocumentItemProcessor(Map<String, SuggestDocument> suggestDocuments, GoRelationFileRepo goRelationFileRepo) {
        return new UniProtEntryDocumentPairProcessor(
                new UniProtEntryConverter(
                        createTaxonomyRepo(),
                        goRelationFileRepo,
                        createPathwayRepo(),
                        ChebiRepoFactory.get(uniProtKBIndexingProperties.getChebiFile()),
                        ECRepoFactory.get(uniProtKBIndexingProperties.getEcDir()),
                        suggestDocuments));
    }

    @Bean
    @StepScope
    ItemReader<UniProtEntryDocumentPair> entryItemReader() {
        return new UniProtEntryItemReader(uniProtKBIndexingProperties);
    }

    @Bean
    UniProtKBIndexingProperties indexingProperties() {
        return uniProtKBIndexingProperties;
    }

    private ItemWriter<Future<UniProtEntryDocumentPair>> asyncWriter(ItemWriter<UniProtEntryDocumentPair> writer) {
        AsyncItemWriter<UniProtEntryDocumentPair> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(writer);

        return asyncItemWriter;
    }

    private ItemProcessor<UniProtEntryDocumentPair, Future<UniProtEntryDocumentPair>> asyncProcessor(ItemProcessor<UniProtEntryDocumentPair, UniProtEntryDocumentPair> itemProcessor) {
        AsyncItemProcessor<UniProtEntryDocumentPair, UniProtEntryDocumentPair> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(itemProcessor);
        asyncProcessor.setTaskExecutor(createItemProcessorTaskExecutor());

        return asyncProcessor;
    }

    private PathwayRepo createPathwayRepo() {
        return new PathwayFileRepo(uniProtKBIndexingProperties.getPathwayFile());
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(uniProtKBIndexingProperties.getTaxonomyFile())));
    }

    private ThreadPoolTaskExecutor createItemProcessorTaskExecutor() {
        TaskExecutorProperties taskExecutorProperties = uniProtKBIndexingProperties
                .getItemProcessorTaskExecutorProperties();
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(taskExecutorProperties.getCorePoolSize());
        taskExecutor.setMaxPoolSize(taskExecutorProperties.getMaxPoolSize());
        taskExecutor.setQueueCapacity(taskExecutorProperties.getQueueCapacity());
        taskExecutor.setKeepAliveSeconds(taskExecutorProperties.getKeepAliveSeconds());
        taskExecutor.setAllowCoreThreadTimeOut(taskExecutorProperties.isAllowCoreThreadTimeout());
        taskExecutor.setWaitForTasksToCompleteOnShutdown(taskExecutorProperties.isWaitForTasksToCompleteOnShutdown());
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();
        taskExecutor.setThreadNamePrefix(taskExecutorProperties.getThreadNamePrefix());
        log.info("Using Item Processor/Writer task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }
}