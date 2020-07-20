package org.uniprot.store.datastore.light.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF_LIGHT_STORE_JOB;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
@Configuration
public class UniRefLightStoreJob {
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public UniRefLightStoreJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Job unirefLightStoreJob(
            @Qualifier("unirefLightStoreMainStep") Step unirefLightStoreMainStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(UNIREF_LIGHT_STORE_JOB)
                .start(unirefLightStoreMainStep)
                .listener(writeRetrierLogJobListener)
                .build();
    }
}
