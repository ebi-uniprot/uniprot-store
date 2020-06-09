package org.uniprot.store.indexer.uniprotkb.step;

import static org.uniprot.store.indexer.common.utils.Constants.INACTIVEENTRY_INDEX_STEP;

import java.util.concurrent.Future;

import net.jodah.failsafe.RetryPolicy;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.uniprot.inactiveentry.FFInactiveUniProtEntryIterator;
import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveEntryIterator;
import org.uniprot.store.indexer.uniprotkb.config.AsyncConfig;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBConfig;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import org.uniprot.store.indexer.uniprotkb.model.InactiveEntryDocumentPair;
import org.uniprot.store.indexer.uniprotkb.processor.InactiveEntryConverter;
import org.uniprot.store.indexer.uniprotkb.processor.InactiveEntryDocumentPairProcessor;
import org.uniprot.store.indexer.uniprotkb.reader.InactiveUniProtEntryItemReader;
import org.uniprot.store.indexer.uniprotkb.writer.InactiveEntryDocumentPairWriter;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.WriteRetrierLogStepListener;
import org.uniprot.store.search.SolrCollection;

/**
 * @author jluo
 * @date: 5 Sep 2019
 */
@Configuration
@Import({UniProtKBConfig.class, AsyncConfig.class})
public class InactiveEntryStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniProtKBIndexingProperties uniProtKBIndexingProperties;
    private final UniProtSolrClient uniProtSolrClient;

    @Autowired
    public InactiveEntryStep(
            StepBuilderFactory stepBuilderFactory,
            UniProtSolrClient uniProtSolrClient,
            UniProtKBIndexingProperties indexingProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniProtSolrClient = uniProtSolrClient;
        this.uniProtKBIndexingProperties = indexingProperties;
    }

    @Bean(name = "inactiveEntryIndexingMainStep")
    public Step inactiveEntryIndexingMainFFStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("inactiveEntry")
                    LogRateListener<InactiveEntryDocumentPair> uniProtKBLogRateListener,
            @Qualifier("inactiveEntryReader") ItemReader<InactiveEntryDocumentPair> entryItemReader,
            @Qualifier("inactiveEntryAsyncProcessor")
                    ItemProcessor<InactiveEntryDocumentPair, Future<InactiveEntryDocumentPair>>
                            asyncProcessor,
            @Qualifier("inactiveEntryAsyncWriter")
                    ItemWriter<Future<InactiveEntryDocumentPair>> asyncWriter,
            InactiveEntryDocumentPairProcessor inactiveEntryDocumentItemProcessor,
            @Qualifier("inactiveEntryWriter")
                    ItemWriter<InactiveEntryDocumentPair> inactiveEntryDocumentItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(INACTIVEENTRY_INDEX_STEP)
                .listener(promotionListener)
                .<InactiveEntryDocumentPair, Future<InactiveEntryDocumentPair>>chunk(
                        uniProtKBIndexingProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(asyncProcessor)
                .writer(asyncWriter)
                .listener(writeRetrierLogStepListener)
                .listener(uniProtKBLogRateListener)
                .listener(inactiveEntryDocumentItemProcessor)
                .listener(unwrapProxy(inactiveEntryDocumentItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean("inactiveEntryReader")
    public ItemReader<InactiveEntryDocumentPair> entryItemReader() {
        return new InactiveUniProtEntryItemReader(inactiveEntryIterator());
    }

    @Bean("inactiveEntryIterator")
    public InactiveEntryIterator inactiveEntryIterator() {
        return new FFInactiveUniProtEntryIterator(
                uniProtKBIndexingProperties.getInactiveEntryFile());
    }
    // ---------------------- Processors ----------------------
    @Bean("inactiveItemProcessor")
    public InactiveEntryDocumentPairProcessor uniProtDocumentItemProcessor() {
        return new InactiveEntryDocumentPairProcessor(new InactiveEntryConverter());
    }

    @Bean("inactiveEntryAsyncProcessor")
    public ItemProcessor<InactiveEntryDocumentPair, Future<InactiveEntryDocumentPair>>
            asyncProcessor(
                    InactiveEntryDocumentPairProcessor uniProtDocumentItemProcessor,
                    @Qualifier("itemProcessorTaskExecutor")
                            ThreadPoolTaskExecutor itemProcessorTaskExecutor) {
        AsyncItemProcessor<InactiveEntryDocumentPair, InactiveEntryDocumentPair> asyncProcessor =
                new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(uniProtDocumentItemProcessor);
        asyncProcessor.setTaskExecutor(itemProcessorTaskExecutor);
        return asyncProcessor;
    }
    // ---------------------- Writers ----------------------
    @Bean("inactiveEntryWriter")
    public ItemWriter<InactiveEntryDocumentPair> uniProtDocumentItemWriter(
            RetryPolicy<Object> writeRetryPolicy) {
        return new InactiveEntryDocumentPairWriter(
                this.uniProtSolrClient, SolrCollection.uniprot, writeRetryPolicy);
    }

    @Bean("inactiveEntryAsyncWriter")
    public ItemWriter<Future<InactiveEntryDocumentPair>> asyncWriter(
            ItemWriter<InactiveEntryDocumentPair> uniProtDocumentItemWriter) {
        AsyncItemWriter<InactiveEntryDocumentPair> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(uniProtDocumentItemWriter);

        return asyncItemWriter;
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "inactiveEntry")
    public LogRateListener<InactiveEntryDocumentPair> uniProtKBLogRateListener() {
        return new LogRateListener<>(uniProtKBIndexingProperties.getUniProtKBLogRateInterval());
    }

    private Object unwrapProxy(Object bean) throws Exception {
        if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
            Advised advised = (Advised) bean;
            bean = advised.getTargetSource().getTarget();
        }
        return bean;
    }
}
