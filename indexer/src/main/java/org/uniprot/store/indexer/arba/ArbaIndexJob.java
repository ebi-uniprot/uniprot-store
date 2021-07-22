package org.uniprot.store.indexer.arba;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.DataSourceConfig;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;
import org.uniprot.store.search.SolrCollection;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
@Configuration
@Import({DataSourceConfig.class, SolrRepositoryConfig.class})
public class ArbaIndexJob {
    private final JobBuilderFactory jobBuilderFactory;
    private final UniProtSolrClient uniProtSolrClient;

    @Autowired
    public ArbaIndexJob(JobBuilderFactory jobBuilderFactory, UniProtSolrClient uniProtSolrClient) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean("arbaLoadJob")
    public Job arbaLoadJob(
            Step arbaProteinCountSQLStep,
            Step indexArbaStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(Constants.ARBA_INDEX_JOB)
                .start(arbaProteinCountSQLStep) // load arba's rule protein count from DB
                .next(indexArbaStep) // index arba entry
                .listener(writeRetrierLogJobListener)
                .listener(
                        new JobExecutionListener() {
                            @Override
                            public void beforeJob(JobExecution jobExecution) {
                                // no-op
                            }

                            @Override
                            public void afterJob(JobExecution jobExecution) {
                                uniProtSolrClient.commit(SolrCollection.arba);
                            }
                        })
                .build();
    }
}
