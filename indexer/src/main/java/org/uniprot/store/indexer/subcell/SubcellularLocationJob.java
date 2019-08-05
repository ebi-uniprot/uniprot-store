package org.uniprot.store.indexer.subcell;

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
 * @since 2019-07-11
 */
@Configuration
@Import({DataSourceConfig.class, SolrRepositoryConfig.class})
public class SubcellularLocationJob {

    @Autowired
    private JobBuilderFactory jobs;

    @Bean("SubcellularLocationLoadJob")
    public Job indexSubcellularLocationSupportingData(@Qualifier("IndexSubcellularLocationStep") Step indexSubcellularLocation,
                                                      @Qualifier("subcellularLocationStatistics") Step subcellularLocationStatistics,
                                                      JobExecutionListener jobListener) {
        return this.jobs.get(Constants.SUBCELLULAR_LOCATION_LOAD_JOB_NAME)
                .start(subcellularLocationStatistics)//index the subcellular location statistics only
                .next(indexSubcellularLocation) // index subcellular location entry
                .listener(jobListener)
                .build();
    }

}
