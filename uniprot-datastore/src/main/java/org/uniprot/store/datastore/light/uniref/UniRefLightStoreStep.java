package org.uniprot.store.datastore.light.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF_LIGHT_STORE_STEP;

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
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.light.uniref.config.UniRefLightAsnycConfig;
import org.uniprot.store.datastore.light.uniref.config.UniRefLightConfig;
import org.uniprot.store.datastore.light.uniref.config.UniRefLightStoreConfig;
import org.uniprot.store.datastore.light.uniref.config.UniRefLightStoreProperties;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.WriteRetrierLogStepListener;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
@Configuration
@Import({UniRefLightStoreConfig.class, UniRefLightConfig.class, UniRefLightAsnycConfig.class})
public class UniRefLightStoreStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniRefLightStoreProperties unirefLightStoreProperties;

    @Autowired
    public UniRefLightStoreStep(
            StepBuilderFactory stepBuilderFactory,
            UniRefLightStoreProperties unirefLightStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.unirefLightStoreProperties = unirefLightStoreProperties;
    }

    @Bean(name = "unirefLightStoreMainStep")
    public Step unirefLightMainStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("unirefLightLogRateListener")
                    LogRateListener<UniRefEntryLight> unirefLightLogRateListener,
            @Qualifier("unirefEntryLightItemReader") ItemReader<Entry> unirefEntryLightItemReader,
            ItemProcessor<Entry, UniRefEntryLight> unirefEntryLightProcessor,
            ItemWriter<UniRefEntryLight> unirefLightEntryItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(UNIREF_LIGHT_STORE_STEP)
                .listener(promotionListener)
                .<Entry, UniRefEntryLight>chunk(unirefLightStoreProperties.getChunkSize())
                .reader(unirefEntryLightItemReader)
                .processor(unirefEntryLightProcessor)
                .writer(unirefLightEntryItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(unirefLightLogRateListener)
                .listener(unwrapProxy(unirefLightEntryItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<Entry> unirefEntryLightItemReader() {
        return new UniRefLightXmlEntryReader(unirefLightStoreProperties.getXmlFilePath());
    }

    // ---------------------- Processors ----------------------
    @Bean
    public ItemProcessor<Entry, UniRefEntryLight> unirefEntryLightProcessor() {
        return new UniRefEntryLightProcessor();
    }

    // ---------------------- Writers ----------------------
    @Bean
    public ItemRetryWriter<UniRefEntryLight, UniRefEntryLight> unirefLightEntryItemWriter(
            UniProtStoreClient<UniRefEntryLight> unirefLightStoreClient,
            RetryPolicy<Object> writeRetryPolicy) {
        return new UniRefEntryLightRetryWriter(
                entries -> entries.forEach(unirefLightStoreClient::saveEntry), writeRetryPolicy);
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "unirefLightLogRateListener")
    public LogRateListener<UniRefEntryLight> unirefLightLogRateListener() {
        return new LogRateListener<>(unirefLightStoreProperties.getLogRateInterval());
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
