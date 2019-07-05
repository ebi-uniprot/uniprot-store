/*
 * Created by sahmad on 29/01/19 11:28
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.indexer.crossref;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.uniprot.indexer.common.config.DataSourceConfig;
import uk.ac.ebi.uniprot.indexer.common.config.SolrRepositoryConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

@Configuration
@Import({SolrRepositoryConfig.class, DataSourceConfig.class})
public class CrossRefJob {

    @Autowired
    private JobBuilderFactory jobs;

    @Bean("indexCrossRefJob")
    public Job indexSupportingData(@Qualifier("IndexCrossRefStep") Step indexCrossRef,
                                   @Qualifier("CrossRefUniProtKBCountStep") Step indexUniProtCount,
                                   JobExecutionListener jobListener) {
        return this.jobs.get(Constants.CROSS_REF_LOAD_JOB)
                .start(indexUniProtCount)// get the cross refs protein count and cache them in a map and use in next step
                .next(indexCrossRef)// index the cross references
                .listener(jobListener)
                .build();
    }

}
