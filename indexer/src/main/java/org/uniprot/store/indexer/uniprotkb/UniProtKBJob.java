package org.uniprot.store.indexer.uniprotkb;

import static org.uniprot.store.indexer.common.utils.Constants.UNIPROTKB_INDEX_JOB;

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
import org.uniprot.store.indexer.common.config.CacheConfig;
import org.uniprot.store.indexer.common.config.SolrRepositoryConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.listener.WriteRetrierLogJobListener;
import org.uniprot.store.search.SolrCollection;

/**
 * The main UniProtKB indexing job.
 * <p>
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({SolrRepositoryConfig.class, CacheConfig.class})
public class UniProtKBJob {
    public static final String GO_ANCESTORS_CACHE = "goAncestorsCache";
    private final JobBuilderFactory jobBuilderFactory;
    private final UniProtSolrOperations solrOperations;

    @Autowired
    public UniProtKBJob(JobBuilderFactory jobBuilderFactory, UniProtSolrOperations solrOperations) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.solrOperations = solrOperations;
    }

    @Bean
    public Job uniProtKBIndexingJob(@Qualifier("uniProtKBIndexingMainStep") Step uniProtKBIndexingMainFFStep,
                                    @Qualifier("suggestionIndexingStep") Step suggestionStep,
                                    WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory.get(UNIPROTKB_INDEX_JOB)
                .start(uniProtKBIndexingMainFFStep)
                .next(suggestionStep)
                .listener(writeRetrierLogJobListener)
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        // no-op
                    }

                    // Hard commit contents of repository once job has finished.
                    // Delegate all other commits to 'autoCommit' element of solrconfig.xml
                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        solrOperations.commit(SolrCollection.uniprot.name());
                    }
                })
                .build();
    }
}
