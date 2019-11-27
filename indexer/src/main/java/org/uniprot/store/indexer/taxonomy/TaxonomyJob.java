package org.uniprot.store.indexer.taxonomy;

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

@Configuration
@Import({DataSourceConfig.class, SolrRepositoryConfig.class})
public class TaxonomyJob {

    @Autowired private JobBuilderFactory jobs;

    @Bean("indexTaxonomyJob")
    public Job indexTaxonomy(
            @Qualifier("taxonomyNode") Step taxonomyNode,
            @Qualifier("taxonomyStatistics") Step taxonomyStatistics,
            @Qualifier("taxonomyMerged") Step taxonomyMerged,
            @Qualifier("taxonomyDeleted") Step taxonomyDeleted,
            JobExecutionListener jobListener) {
        return this.jobs
                .get(Constants.TAXONOMY_LOAD_JOB_NAME)
                .start(taxonomyDeleted)
                .next(taxonomyMerged)
                .next(taxonomyStatistics)
                .next(taxonomyNode)
                .listener(jobListener)
                .build();
    }
}
