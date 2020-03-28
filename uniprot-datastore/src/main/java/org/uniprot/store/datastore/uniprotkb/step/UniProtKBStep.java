package org.uniprot.store.datastore.uniprotkb.step;

import static org.uniprot.store.datastore.utils.Constants.UNIPROTKB_STORE_STEP;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.uniprotkb.config.AsyncConfig;
import org.uniprot.store.datastore.uniprotkb.config.StoreConfig;
import org.uniprot.store.datastore.uniprotkb.config.UniProtKBConfig;
import org.uniprot.store.datastore.uniprotkb.config.UniProtKBStoreProperties;
import org.uniprot.store.datastore.uniprotkb.reader.UniProtEntryItemReader;
import org.uniprot.store.datastore.uniprotkb.writer.UniProtEntryRetryWriter;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.WriteRetrierLogStepListener;

/**
 * The main UniProtKB store step.
 *
 * <p>Created 10/04/19
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
    public UniProtKBStep(
            StepBuilderFactory stepBuilderFactory,
            UniProtKBStoreProperties uniProtKBStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniProtKBStoreProperties = uniProtKBStoreProperties;
    }

    @Bean(name = "uniProtKBStoreMainStep")
    public Step uniProtKBStoreMainStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("uniProtKB") LogRateListener<UniProtKBEntry> uniProtKBLogRateListener,
            ItemReader<UniProtKBEntry> entryItemReader,
            ItemProcessor<UniProtKBEntry, UniProtKBEntry> uniProtEntryPassThroughProcessor,
            ItemWriter<UniProtKBEntry> uniProtEntryItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(UNIPROTKB_STORE_STEP)
                .listener(promotionListener)
                .<UniProtKBEntry, UniProtKBEntry>chunk(uniProtKBStoreProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(uniProtEntryPassThroughProcessor)
                .writer(uniProtEntryItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(uniProtKBLogRateListener)
                .listener(unwrapProxy(uniProtEntryItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<UniProtKBEntry> entryItemReader() {
        return new UniProtEntryItemReader(uniProtKBStoreProperties);
    }

    // ---------------------- Processors ----------------------
    @Bean
    public ItemProcessor<UniProtKBEntry, UniProtKBEntry> uniProtEntryPassThroughProcessor() {
        return new PassThroughItemProcessor<>();
    }

    // ---------------------- Writers ----------------------
    @Bean
    public ItemWriter<UniProtKBEntry> uniProtEntryItemWriter(
            UniProtStoreClient<UniProtKBEntry> uniProtKBStoreClient,
            RetryPolicy<Object> writeRetryPolicy) {
        return new UniProtEntryRetryWriter(
                entries -> entries.forEach(uniProtKBStoreClient::saveEntry), writeRetryPolicy);
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "uniProtKB")
    public LogRateListener<UniProtKBEntry> uniProtKBLogRateListener() {
        return new LogRateListener<>(uniProtKBStoreProperties.getUniProtKBLogRateInterval());
    }

    // ---------------------- Source Data Access beans and helpers ----------------------

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
