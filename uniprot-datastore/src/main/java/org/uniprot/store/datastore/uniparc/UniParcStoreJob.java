package org.uniprot.store.datastore.uniparc;

import static org.uniprot.store.datastore.utils.Constants.UNIPARC_STORE_JOB;

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
 * @since 2020-03-03
 */
@Configuration
public class UniParcStoreJob {

    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public UniParcStoreJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Job uniParcStoreJob(
            @Qualifier("uniParcStoreMainStep") Step uniParcStoreMainStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(UNIPARC_STORE_JOB)
                .start(uniParcStoreMainStep)
                .listener(writeRetrierLogJobListener)
                .build();
    }
}
