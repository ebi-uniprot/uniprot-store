package org.uniprot.store.indexer.genecentric;

import static org.uniprot.store.indexer.common.utils.Constants.GENE_CENTRIC_INDEX_JOB;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;
import org.uniprot.store.search.SolrCollection;

/**
 * This Job will index Gene Centric data into solr in 2 steps. Step1: load canonical proteins gene
 * centric fasta files and index it in solr (geneCentricCanonicalIndexStep) Step2: load related
 * proteins gene centric fasta files, in the processor, it will fetch previously saved canonical
 * entry and add the related protein to its canonical entry and update the solr document in solr.
 *
 * @author lgonzales
 * @since 02/11/2020
 */
@Configuration
@Import({SolrRepositoryConfig.class})
public class GeneCentricJob {

    private final JobBuilderFactory jobBuilderFactory;
    private final UniProtSolrClient uniProtSolrClient;

    @Autowired
    public GeneCentricJob(
            JobBuilderFactory jobBuilderFactory, UniProtSolrClient uniProtSolrClient) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.uniProtSolrClient = uniProtSolrClient;
    }

    @Bean
    public Job geneCentricIndexJob(
            @Qualifier("geneCentricCanonicalIndexStep") Step geneCentricCanonicalIndexStep,
            @Qualifier("geneCentricRelatedIndexStep") Step geneCentricRelatedIndexStep,
            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory
                .get(GENE_CENTRIC_INDEX_JOB)
                .start(geneCentricCanonicalIndexStep)
                .next(geneCentricRelatedIndexStep)
                .listener(writeRetrierLogJobListener)
                .listener(
                        new JobExecutionListener() {
                            @Override
                            public void beforeJob(JobExecution jobExecution) {
                                // no-op
                            }

                            @Override
                            public void afterJob(JobExecution jobExecution) {
                                uniProtSolrClient.commit(SolrCollection.genecentric);
                            }
                        })
                .build();
    }
}
