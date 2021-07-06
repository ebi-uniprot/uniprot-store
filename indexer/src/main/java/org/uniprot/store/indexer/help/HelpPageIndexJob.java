package org.uniprot.store.indexer.help;

import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;

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
}
