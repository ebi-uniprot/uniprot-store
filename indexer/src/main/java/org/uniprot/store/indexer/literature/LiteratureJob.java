package org.uniprot.store.indexer.literature;

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

/** @author lgonzales */
@Configuration
@Import({DataSourceConfig.class, SolrRepositoryConfig.class})
public class LiteratureJob {
    @Autowired private JobBuilderFactory jobs;

    @Bean("LiteratureLoadJob")
    public Job indexLiteratureSupportingData(
            @Qualifier("IndexLiteratureStep") Step indexLiterature,
            @Qualifier("LiteratureMappingStep") Step literatureMappingStep,
            @Qualifier("LiteratureStatistics") Step literatureStatistics,
            JobExecutionListener jobListener) {
        return this.jobs
                .get(Constants.LITERATURE_LOAD_JOB_NAME)
                //                .start(literatureStatistics) // DO NOT MERGE TO MASTER COVID-19
                // CODE
                .start(literatureMappingStep) // update all pir mappings
                .next(indexLiterature) // index literature entry
                .listener(jobListener)
                .build();
    }
}
