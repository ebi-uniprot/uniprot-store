package org.uniprot.store.datastore.member.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF_MEMBER_STORE_JOB;

import java.util.List;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.member.uniref.config.UniRefMemberStoreProperties;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Configuration
public class UniRefMemberStoreJob {
    private final JobBuilderFactory jobBuilderFactory;
    private final UniRefMemberStoreProperties unirefMemberStoreProperties;

    @Autowired
    public UniRefMemberStoreJob(
            JobBuilderFactory jobBuilderFactory,
            UniRefMemberStoreProperties unirefMemberStoreProperties) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.unirefMemberStoreProperties = unirefMemberStoreProperties;
    }

    @Bean
    public Job unirefMemberStoreJob(
            @Qualifier("uniref100MembersStoreStep") Step uniref100MembersStoreStep,
            @Qualifier("uniref90MembersStoreStep") Step uniref90MembersStoreStep,
            @Qualifier("uniref50MembersStoreStep") Step uniref50MembersStoreStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(UNIREF_MEMBER_STORE_JOB)
                .start(uniref100MembersStoreStep)
                .next(uniref90MembersStoreStep)
                .next(uniref50MembersStoreStep)
                .listener(writeRetrierLogJobListener)
                .build();
    }

    // ---------------------- Listeners ----------------------
    @Bean(name = "unirefMemberLogRateListener")
    public LogRateListener<RepresentativeMember> unirefMemberLogRateListener() {
        return new LogRateListener<>(unirefMemberStoreProperties.getLogRateInterval());
    }

    @Bean(name = "uniref90And50MemberLogRateListener")
    public LogRateListener<List<RepresentativeMember>> uniref90And50MemberLogRateListener() {
        return new UniRef90And50MemberLogRateListener(
                unirefMemberStoreProperties.getLogRateInterval());
    }

    // ---------------------- Processors ----------------------
    @Bean(name = "uniref90And50MemberProcessor")
    public ItemProcessor<List<MemberType>, List<RepresentativeMember>> uniref90And50MemberProcessor(
            UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient) {
        return new UniRef90And50MemberProcessor(unirefMemberStoreClient);
    }

    public static Object unwrapProxy(Object bean) throws Exception {
        if (AopUtils.isAopProxy(bean) && bean instanceof Advised) {
            Advised advised = (Advised) bean;
            bean = advised.getTargetSource().getTarget();
        }
        return bean;
    }
}
