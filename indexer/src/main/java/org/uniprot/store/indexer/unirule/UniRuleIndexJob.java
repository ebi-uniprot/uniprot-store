package org.uniprot.store.indexer.unirule;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.store.indexer.common.utils.Constants;

@Configuration
public class UniRuleIndexJob {
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public UniRuleIndexJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Job uniRuleIndexJob(
            Step proteinCountStep, Step indexUniRuleStep, JobExecutionListener jobListener) {
        return this.jobBuilderFactory
                .get(Constants.UNIRULE_INDEX_JOB)
                .start(
                        proteinCountStep) // create a map of old rule id to total protein-annotated
                                          // count
                .next(indexUniRuleStep) // index uniRule entry
                .listener(jobListener)
                .build();
    }
}
