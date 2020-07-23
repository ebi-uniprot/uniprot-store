package org.uniprot.store.datastore.member.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF_MEMBER_STORE_JOB;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Configuration
public class UniRefMemberStoreJob {
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public UniRefMemberStoreJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Job unirefMemberStoreJob(
            @Qualifier("unirefMemberStoreMainStep") Step unirefMemberStoreMainStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(UNIREF_MEMBER_STORE_JOB)
                .start(unirefMemberStoreMainStep)
                .listener(writeRetrierLogJobListener)
                .build();
    }
}
