package org.uniprot.store.datastore.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF_STORE_STEP;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
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
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.uniref.config.UniRefAsnycConfig;
import org.uniprot.store.datastore.uniref.config.UniRefConfig;
import org.uniprot.store.datastore.uniref.config.UniRefStoreConfig;
import org.uniprot.store.datastore.uniref.config.UniRefStoreProperties;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.WriteRetrierLogStepListener;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

import net.jodah.failsafe.RetryPolicy;

/**
 * @author jluo
 * @date: 15 Aug 2019
 */
@Configuration
@Import({UniRefStoreConfig.class, UniRefConfig.class, UniRefAsnycConfig.class})
public class UniRefStoreStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniRefStoreProperties unirefStoreProperties;

    @Autowired
    public UniRefStoreStep(
            StepBuilderFactory stepBuilderFactory, UniRefStoreProperties unirefStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.unirefStoreProperties = unirefStoreProperties;
    }

    @Bean(name = "unirefStoreMainStep")
    public Step unirefMainStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("unirefLogRateListener") LogRateListener<UniRefEntry> unirefLogRateListener,
            ItemReader<Entry> entryItemReader,
            ItemProcessor<Entry, UniRefEntry> unirefEntryProcessor,
            ItemWriter<UniRefEntry> unirefEntryItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(UNIREF_STORE_STEP)
                .listener(promotionListener)
                .<Entry, UniRefEntry>chunk(unirefStoreProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(unirefEntryProcessor)
                .writer(unirefEntryItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(unirefLogRateListener)
                .listener(unwrapProxy(unirefEntryItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<Entry> unirefEntryItemReader() {
        return new UniRefXmlEntryReader(unirefStoreProperties.getXmlFilePath());
    }

    // ---------------------- Processors ----------------------
    @Bean
    public ItemProcessor<Entry, UniRefEntry> unirefEntryProcessor() {
        return new UniRefEntryProcessor();
    }

    // ---------------------- Writers ----------------------
    @Bean
    public ItemRetryWriter<UniRefEntry, UniRefEntry> unirefEntryItemWriter(
            UniProtStoreClient<UniRefEntry> unirefStoreClient,
            RetryPolicy<Object> writeRetryPolicy) {
        return new UniRefEntryRetryWriter(
                entries -> entries.forEach(unirefStoreClient::saveEntry), writeRetryPolicy);
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "unirefLogRateListener")
    public LogRateListener<UniRefEntry> unirefLogRateListener() {
        return new LogRateListener<>(unirefStoreProperties.getLogRateInterval());
    }
    // g
    private Object unwrapProxy(Object bean) throws Exception {
        if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
            Advised advised = (Advised) bean;
            bean = advised.getTargetSource().getTarget();
        }
        return bean;
    }
}
