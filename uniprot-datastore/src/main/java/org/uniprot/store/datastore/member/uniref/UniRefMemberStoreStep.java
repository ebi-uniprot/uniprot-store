package org.uniprot.store.datastore.member.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF_MEMBER_STORE_STEP;

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
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.member.uniref.config.UniRefMemberAsnycConfig;
import org.uniprot.store.datastore.member.uniref.config.UniRefMemberConfig;
import org.uniprot.store.datastore.member.uniref.config.UniRefMemberStoreConfig;
import org.uniprot.store.datastore.member.uniref.config.UniRefMemberStoreProperties;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.WriteRetrierLogStepListener;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Configuration
@Import({UniRefMemberStoreConfig.class, UniRefMemberConfig.class, UniRefMemberAsnycConfig.class})
public class UniRefMemberStoreStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniRefMemberStoreProperties unirefMemberStoreProperties;

    @Autowired
    public UniRefMemberStoreStep(
            StepBuilderFactory stepBuilderFactory,
            UniRefMemberStoreProperties unirefMemberStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.unirefMemberStoreProperties = unirefMemberStoreProperties;
    }

    @Bean(name = "unirefMemberStoreMainStep")
    public Step unirefMemberMainStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("unirefMemberLogRateListener")
                    LogRateListener<RepresentativeMember> unirefMemberLogRateListener,
            @Qualifier("unirefMemberItemReader") ItemReader<MemberType> unirefMemberItemReader,
            ItemProcessor<MemberType, RepresentativeMember> unirefMemberProcessor,
            ItemWriter<RepresentativeMember> unirefMemberItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(UNIREF_MEMBER_STORE_STEP)
                .listener(promotionListener)
                .<MemberType, RepresentativeMember>chunk(unirefMemberStoreProperties.getChunkSize())
                .reader(unirefMemberItemReader)
                .processor(unirefMemberProcessor)
                .writer(unirefMemberItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(unirefMemberLogRateListener)
                .listener(unwrapProxy(unirefMemberItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<MemberType> unirefMemberItemReader() {
        return new UniRefMemberXmlEntryReader(unirefMemberStoreProperties.getXmlFilePath());
    }

    // ---------------------- Processors ----------------------
    @Bean
    public ItemProcessor<MemberType, RepresentativeMember> unirefEntryMemberProcessor() {
        return new UniRefMemberProcessor();
    }

    // ---------------------- Writers ----------------------
    @Bean
    public ItemRetryWriter<RepresentativeMember, RepresentativeMember> unirefMemberEntryItemWriter(
            UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient,
            RetryPolicy<Object> writeRetryPolicy) {
        return new UniRefMemberRetryWriter(
                entries -> entries.forEach(unirefMemberStoreClient::saveEntry), writeRetryPolicy);
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "unirefMemberLogRateListener")
    public LogRateListener<RepresentativeMember> unirefMemberLogRateListener() {
        return new LogRateListener<>(unirefMemberStoreProperties.getLogRateInterval());
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
