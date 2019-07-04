/*
 * Created by sahmad on 29/01/19 11:28
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.indexer.disease;

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
@Import({DataSourceConfig.class, SolrRepositoryConfig.class})
public class DiseaseLoadJob {
    @Autowired
    private JobBuilderFactory jobs;

    @Bean("DiseaseLoadJob")
    public Job indexSupportingData(@Qualifier("DiseaseProteinCountStep") Step diseaseProteinCountStep,
            @Qualifier("IndexDiseaseStep") Step indexDisease,
                                   JobExecutionListener jobListener) {
        return this.jobs.get(Constants.DISEASE_LOAD_JOB_NAME)
                .start(diseaseProteinCountStep)// get the protein count of the diseases and cache them for next step
                .next(indexDisease)//index the disease
                .listener(jobListener)
                .build();
    }
}
