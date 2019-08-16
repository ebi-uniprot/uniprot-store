package org.uniprot.store.datastore.uniref;

import static org.uniprot.store.datastore.utils.Constants.UNIREF_STORE_JOB;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;

/**
 *
 * @author jluo
 * @date: 16 Aug 2019
 *
*/
@Configuration
public class UniRefStoreJob {
	 private final JobBuilderFactory jobBuilderFactory;

	    @Autowired
	    public UniRefStoreJob(JobBuilderFactory jobBuilderFactory) {
	        this.jobBuilderFactory = jobBuilderFactory;
	    }

	    @Bean
	    public Job unirefStoreJob(@Qualifier("unirefStoreMainStep") Step unirefStoreMainStep,
	                                    WriteRetrierLogJobListener writeRetrierLogJobListener) {
	        return this.jobBuilderFactory.get(UNIREF_STORE_JOB)
	                .start(unirefStoreMainStep)
	                .listener(writeRetrierLogJobListener)
	                .build();
	    }
}

