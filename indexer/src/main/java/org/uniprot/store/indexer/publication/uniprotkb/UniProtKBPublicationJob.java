package org.uniprot.store.indexer.publication.uniprotkb;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * @author sahmad
 * @created 16/12/2020
 */
@Configuration
@Import({SolrRepositoryConfig.class})
public class UniProtKBPublicationJob {
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public UniProtKBPublicationJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean("indexUniProtKBPublicationJob")
    public Job indexUniProtKBPublicationData(
            @Qualifier("indexUniProtKBPublicationStep") Step indexUniProtKBPublicationStep,
            JobExecutionListener jobListener) {
        return this.jobBuilderFactory
                .get(Constants.UNIPROTKB_PUBLICATION_JOB_NAME)
                .start(indexUniProtKBPublicationStep)
                .listener(jobListener)
                .build();
    }
}
