package uk.ac.ebi.uniprot.indexer.taxonomy;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

@Configuration
public class TaxonomyJob {

    @Autowired
    private JobBuilderFactory jobs;

    @Bean("indexTaxonomyJob")
    public Job indexTaxonomy(Step taxonomyNode, Step taxonomyStrain, Step taxonomyVirusHost, Step taxonomyNames,
                             Step taxonomyURL, Step taxonomyCount, JobExecutionListener jobListener) {
        return this.jobs.get(Constants.TAXONOMY_LOAD_JOB_NAME)
                .start(taxonomyNode)
                .next(taxonomyStrain)
                .next(taxonomyVirusHost)
                .next(taxonomyURL)
                .next(taxonomyNames)
                .next(taxonomyCount)
                .listener(jobListener)
                .build();
    }
}
