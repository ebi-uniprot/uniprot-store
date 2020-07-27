package org.uniprot.store.datastore.member.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF100_MEMBER_STORE_STEP;

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
public class UniRef100MembersStoreStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniRefMemberStoreProperties unirefMemberStoreProperties;

    @Autowired
    public UniRef100MembersStoreStep(
            StepBuilderFactory stepBuilderFactory,
            UniRefMemberStoreProperties unirefMemberStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.unirefMemberStoreProperties = unirefMemberStoreProperties;
    }

    @Bean(name = "uniref100MembersStoreStep")
    public Step uniref100MembersStoreStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("unirefMemberLogRateListener")
                    LogRateListener<RepresentativeMember> unirefMemberLogRateListener,
            @Qualifier("uniref100MemberItemReader")
                    ItemReader<MemberType> uniref100MemberItemReader,
            ItemProcessor<MemberType, RepresentativeMember> uniref100MemberProcessor,
            ItemWriter<RepresentativeMember> uniref100MemberItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(UNIREF100_MEMBER_STORE_STEP)
                .listener(promotionListener)
                .<MemberType, RepresentativeMember>chunk(unirefMemberStoreProperties.getChunkSize())
                .reader(uniref100MemberItemReader)
                .processor(uniref100MemberProcessor)
                .writer(uniref100MemberItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(unirefMemberLogRateListener)
                .listener(unwrapProxy(uniref100MemberItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<MemberType> uniref100MemberItemReader() {
        return new UniRefMemberXmlEntryReader(
                unirefMemberStoreProperties.getUniref100XmlFilePath());
    }

    // ---------------------- Processors ----------------------
    @Bean
    public ItemProcessor<MemberType, RepresentativeMember> uniref100MemberProcessor() {
        return new UniRef100MemberProcessor();
    }

    // ---------------------- Writers ----------------------
    @Bean
    public ItemRetryWriter<RepresentativeMember, RepresentativeMember> uniref100MemberItemWriter(
            UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient,
            RetryPolicy<Object> writeRetryPolicy) {
        return new UniRefMemberRetryWriter(
                entries -> entries.forEach(unirefMemberStoreClient::saveEntry), writeRetryPolicy);
    }

    private Object unwrapProxy(Object bean) throws Exception {
        if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
            Advised advised = (Advised) bean;
            bean = advised.getTargetSource().getTarget();
        }
        return bean;
    }
}
