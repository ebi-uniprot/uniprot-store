package org.uniprot.store.indexer.uniparc;

import static org.uniprot.store.indexer.common.utils.Constants.UNIPARC_INDEX_JOB;

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
 * @date: 18 Jun 2019
 */
@Configuration
@Import({SolrRepositoryConfig.class})
public class UniParcIndexJob {
    private final JobBuilderFactory jobBuilderFactory;
    private final UniProtSolrClient solrClient;

    @Autowired
    public UniParcIndexJob(JobBuilderFactory jobBuilderFactory, UniProtSolrClient solrClient) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.solrClient = solrClient;
    }

    @Bean
    public Job uniparcIndexingJob(
            @Qualifier("UniParcIndexStep") Step uniparcIndexStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(UNIPARC_INDEX_JOB)
                .start(uniparcIndexStep)
                .listener(writeRetrierLogJobListener)
                .listener(
                        new JobExecutionListener() {
                            @Override
                            public void beforeJob(JobExecution jobExecution) {
                                // no-op
                            }

                            @Override
                            public void afterJob(JobExecution jobExecution) {
                                solrClient.commit(SolrCollection.uniparc);
                            }
                        })
                .build();
    }
}
