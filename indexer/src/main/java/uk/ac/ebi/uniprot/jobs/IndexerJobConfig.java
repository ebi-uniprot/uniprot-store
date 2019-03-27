/*
 * Created by sahmad on 29/01/19 11:28
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.listeners.LogJobListener;
import uk.ac.ebi.uniprot.listeners.LogStepListener;
import uk.ac.ebi.uniprot.utils.Constants;

@Configuration
@EnableBatchProcessing
public class IndexerJobConfig {
    @Autowired
    private JobBuilderFactory jobs;

    @Bean
    public Job indexSupportingData(Step indexCrossRef) {
        return this.jobs.get(Constants.SUPPORTING_DATA_INDEX_JOB)
                .incrementer(new RunIdIncrementer())
                .start(indexCrossRef)//index the cross references
                .listener(jobListener())
                .build();
    }

    @Bean
    public JobExecutionListener jobListener() {
        return new LogJobListener();

    }

    @Bean
    public StepExecutionListener stepListener(){
        return new LogStepListener();
    }
}
