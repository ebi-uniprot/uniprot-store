/*
 * Created by sahmad on 29/01/19 11:28
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package org.uniprot.store.indexer.publication.computational;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.publication.common.LargeScaleSolrFieldName;
import org.uniprot.store.indexer.publication.common.PublicationJobExecutionListener;

@Configuration
@Import({SolrRepositoryConfig.class})
public class ComputationalPublicationJob {
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public ComputationalPublicationJob(
            JobBuilderFactory jobBuilderFactory, UniProtSolrClient uniProtSolrClient) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean("indexComputationalPublicationJob")
    public Job indexSupportingData(
            @Qualifier("cacheLargeScaleStep") Step cacheLargeScaleStep,
            @Qualifier("IndexComputationalPublicationStep") Step indexComputationalPublicationStep,
            PublicationJobExecutionListener publicationJobExecutionListener) {
        return this.jobBuilderFactory
                .get(Constants.COMPUTATIONAL_PUBLICATION_JOB_NAME)
                .start(cacheLargeScaleStep)
                .next(indexComputationalPublicationStep)
                .listener(publicationJobExecutionListener)
                .build();
    }

    @Bean("largeScaleSolrFieldName")
    public LargeScaleSolrFieldName largeScaleSolrFieldName() {
        return LargeScaleSolrFieldName.COMPUTATIONAL;
    }
}
