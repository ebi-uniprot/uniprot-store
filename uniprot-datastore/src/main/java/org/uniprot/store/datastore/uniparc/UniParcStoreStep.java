package org.uniprot.store.datastore.uniparc;

import static org.uniprot.store.datastore.utils.Constants.UNIPARC_STORE_STEP;

import net.jodah.failsafe.RetryPolicy;

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
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.uniparc.config.UniParcAsnycConfig;
import org.uniprot.store.datastore.uniparc.config.UniParcConfig;
import org.uniprot.store.datastore.uniparc.config.UniParcStoreConfig;
import org.uniprot.store.datastore.uniparc.config.UniParcStoreProperties;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.WriteRetrierLogStepListener;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
@Configuration
@Import({UniParcStoreConfig.class, UniParcConfig.class, UniParcAsnycConfig.class})
public class UniParcStoreStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniParcStoreProperties uniParcStoreProperties;

    @Autowired
    public UniParcStoreStep(
            StepBuilderFactory stepBuilderFactory, UniParcStoreProperties uniParcStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.uniParcStoreProperties = uniParcStoreProperties;
    }

    @Bean(name = "uniParcStoreMainStep")
    public Step uniParcStoreMainStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("uniParcLogRateListener")
                    LogRateListener<UniParcEntry> uniParcLogRateListener,
            ItemReader<Entry> entryItemReader,
            ItemProcessor<Entry, UniParcEntry> uniParcEntryProcessor,
            ItemWriter<UniParcEntry> uniParcEntryItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(UNIPARC_STORE_STEP)
                .listener(promotionListener)
                .<Entry, UniParcEntry>chunk(uniParcStoreProperties.getChunkSize())
                .reader(entryItemReader)
                .processor(uniParcEntryProcessor)
                .writer(uniParcEntryItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(uniParcLogRateListener)
                .listener(unwrapProxy(uniParcEntryItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<Entry> uniParcEntryItemReader() {
        return new UniParcXmlEntryReader(uniParcStoreProperties.getXmlFilePath());
    }

    // ---------------------- Processors ----------------------
    @Bean
    public ItemProcessor<Entry, UniParcEntry> uniParcEntryProcessor() {
        return new UniParcEntryProcessor();
    }

    // ---------------------- Writers ----------------------
    @Bean
    public ItemRetryWriter<UniParcEntry, UniParcEntry> uniParcEntryItemWriter(
            UniProtStoreClient<UniParcEntry> uniParcStoreClient,
            RetryPolicy<Object> writeRetryPolicy) {
        return new UniParcEntryRetryWriter(
                entries -> entries.forEach(uniParcStoreClient::saveEntry), writeRetryPolicy);
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "uniParcLogRateListener")
    public LogRateListener<UniParcEntry> uniParcLogRateListener() {
        return new LogRateListener<>(uniParcStoreProperties.getLogRateInterval());
    }

    private Object unwrapProxy(Object bean) throws Exception {
        if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
            Advised advised = (Advised) bean;
            bean = advised.getTargetSource().getTarget();
        }
        return bean;
    }
}
