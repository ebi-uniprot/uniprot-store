package org.uniprot.store.indexer.help;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * @author sahmad
 * @created 06/07/2021
 */
@Configuration
@Import({SolrRepositoryConfig.class})
public class HelpPageIndexJob {

    private final JobBuilderFactory jobBuilderFactory;

    public HelpPageIndexJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean("indexHelpPagesJob")
    public Job indexSupportingData(
            @Qualifier("IndexHelpPageStep") Step indexHelpPageStep,
            JobExecutionListener jobListener) {
        return this.jobBuilderFactory
                .get(Constants.HELP_PAGE_INDEX_JOB_NAME)
                .start(indexHelpPageStep)
                .listener(jobListener)
                .build();
    }
}
