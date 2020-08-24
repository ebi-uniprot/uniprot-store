package org.uniprot.store.indexer.proteome;

import static org.uniprot.store.indexer.common.utils.Constants.PROTEOME_INDEX_JOB;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;
import org.uniprot.store.search.SolrCollection;

/**
 * @author jluo
 * @date: 18 Apr 2019
 */
@Configuration
@Import({SolrRepositoryConfig.class})
public class ProteomeIndexJob {
    private final JobBuilderFactory jobBuilderFactory;
    private final UniProtSolrClient uniProtSolrClient;

    @Autowired
    public ProteomeIndexJob(
            JobBuilderFactory jobBuilderFactory, UniProtSolrClient uniProtSolrClient) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean
    public Job proteomeIndexingJob(
            @Qualifier("ProteomeIndexStep") Step proteomeIndexStep,
            @Qualifier("suggestionProteomeIndexingStep") Step proteomeSuggestionStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(PROTEOME_INDEX_JOB)
                .start(proteomeIndexStep)
                .next(proteomeSuggestionStep)
                .listener(writeRetrierLogJobListener)
                .listener(
                        new JobExecutionListener() {
                            @Override
                            public void beforeJob(JobExecution jobExecution) {
                                // no-op
                            }

                            @Override
                            public void afterJob(JobExecution jobExecution) {
                                uniProtSolrClient.commit(SolrCollection.proteome);
                            }
                        })
                .build();
    }
}
