package org.uniprot.store.indexer.publication.count;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * Created 17/12/2020
 *
 * @author Edd
 */
@Configuration
@Import({SolrRepositoryConfig.class})
public class ProteinCountsJob {

    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public ProteinCountsJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean("indexPublicationsStatsJob")
    public Job indexPublicationsStatsData(
            @Qualifier("indexPublicationStatsStep") Step indexPublicationStatsStep,
            JobExecutionListener jobListener) {
        return this.jobBuilderFactory
                .get(Constants.PUBLICATIONS_STATS_JOB_NAME)
                .start(indexPublicationStatsStep)
                .listener(jobListener)
                .build();
    }
}
