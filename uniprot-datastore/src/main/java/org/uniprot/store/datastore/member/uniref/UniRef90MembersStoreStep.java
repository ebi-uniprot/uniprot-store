package org.uniprot.store.datastore.member.uniref;

import net.jodah.failsafe.RetryPolicy;
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

import java.util.Collection;
import java.util.List;

import static org.uniprot.store.datastore.member.uniref.UniRefMemberStoreJob.unwrapProxy;
import static org.uniprot.store.datastore.utils.Constants.UNIREF90_MEMBER_STORE_STEP;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Configuration
@Import({UniRefMemberStoreConfig.class, UniRefMemberConfig.class, UniRefMemberAsnycConfig.class})
public class UniRef90MembersStoreStep {
    private final StepBuilderFactory stepBuilderFactory;
    private final UniRefMemberStoreProperties unirefMemberStoreProperties;

    @Autowired
    public UniRef90MembersStoreStep(
            StepBuilderFactory stepBuilderFactory,
            UniRefMemberStoreProperties unirefMemberStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.unirefMemberStoreProperties = unirefMemberStoreProperties;
    }

    @Bean(name = "uniref90MembersStoreStep")
    public Step uniref90MembersStoreStep(
            WriteRetrierLogStepListener writeRetrierLogStepListener,
            @Qualifier("uniref90And50MemberLogRateListener")
                    LogRateListener<List<RepresentativeMember>> uniref90And50MemberLogRateListener,
            @Qualifier("uniref90MemberItemReader")
                    ItemReader<List<MemberType>> uniref90MemberItemReader,
            ItemProcessor<List<MemberType>, List<RepresentativeMember>> uniref90And50MemberProcessor,
            ItemWriter<List<RepresentativeMember>> uniref90MemberItemWriter,
            ExecutionContextPromotionListener promotionListener)
            throws Exception {

        return this.stepBuilderFactory
                .get(UNIREF90_MEMBER_STORE_STEP)
                .listener(promotionListener)
                .<List<MemberType>, List<RepresentativeMember>>chunk(
                        unirefMemberStoreProperties.getBatchCount())
                .reader(uniref90MemberItemReader)
                .processor(uniref90And50MemberProcessor)
                .writer(uniref90MemberItemWriter)
                .listener(writeRetrierLogStepListener)
                .listener(uniref90And50MemberLogRateListener)
                .listener(unwrapProxy(uniref90MemberItemWriter))
                .build();
    }

    // ---------------------- Readers ----------------------
    @Bean(name = "uniref90MemberItemReader")
    public ItemReader<List<MemberType>> uniref90MemberItemReader() {
        return new UniRef90And50MembersXmlEntryReader(
                unirefMemberStoreProperties.getUniref90XmlFilePath(),
                unirefMemberStoreProperties.getBatchSize());
    }

    // ---------------------- Writers ----------------------
    @Bean(name = "uniref90MemberItemWriter")
    public ItemRetryWriter<List<RepresentativeMember>, List<RepresentativeMember>>
    uniref90MemberItemWriter(
            UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient,
            RetryPolicy<Object> writeRetryPolicy) {
        return new UniRef90And50MemberRetryWriter(
                entriesList ->
                        entriesList.stream()
                                .flatMap(Collection::stream)
                                .forEach(unirefMemberStoreClient::saveOrUpdateEntry),
                writeRetryPolicy);
    }
}
