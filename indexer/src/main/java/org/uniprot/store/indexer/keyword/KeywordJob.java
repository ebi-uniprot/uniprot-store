package org.uniprot.store.indexer.keyword;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.DataSourceConfig;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * @author lgonzales
 */
@Configuration
@Import({DataSourceConfig.class, SolrRepositoryConfig.class})
public class KeywordJob {
    @Autowired private JobBuilderFactory jobs;

    @Bean("KeywordLoadJob")
    public Job indexKeywordSupportingData(
            @Qualifier("IndexKeywordStep") Step indexKeyword,
            @Qualifier("keywordStatistics") Step keywordStatistics,
            JobExecutionListener jobListener) {
        return this.jobs
                .get(Constants.KEYWORD_LOAD_JOB_NAME)
                .start(keywordStatistics) // index the keyword statistics only
                .next(indexKeyword) // index keyword entry
                .listener(jobListener)
                .build();
    }
}
