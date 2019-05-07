package uk.ac.ebi.uniprot.indexer.taxonomy;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

@Configuration
public class TaxonomyJob {

    @Autowired
    private JobBuilderFactory jobs;

    @Bean("indexTaxonomyJob")
    public Job indexTaxonomy(@Qualifier("taxonomyNode") Step taxonomyNode,
                             @Qualifier("taxonomyStatistics")Step taxonomyStatistics,
                             JobExecutionListener jobListener) {
        return this.jobs.get(Constants.TAXONOMY_LOAD_JOB_NAME)
                .start(taxonomyNode)
                .next(taxonomyStatistics)
                .listener(jobListener)
                .build();
    }
}
