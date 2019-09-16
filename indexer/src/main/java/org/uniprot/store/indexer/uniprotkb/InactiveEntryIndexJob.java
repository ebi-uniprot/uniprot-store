package org.uniprot.store.indexer.uniprotkb;

import static org.uniprot.store.indexer.common.utils.Constants.INACTIVEENTRY_INDEX_JOB;

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
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.job.common.listener.WriteRetrierLogJobListener;
import org.uniprot.store.search.SolrCollection;

/**
 *
 * @author jluo
 * @date: 5 Sep 2019
 *
*/
@Configuration
@Import({SolrRepositoryConfig.class})
public class InactiveEntryIndexJob {
	 private final JobBuilderFactory jobBuilderFactory;
	    private final UniProtSolrOperations solrOperations;

	    @Autowired
	    public InactiveEntryIndexJob(JobBuilderFactory jobBuilderFactory, UniProtSolrOperations solrOperations) {
	        this.jobBuilderFactory = jobBuilderFactory;
	        this.solrOperations = solrOperations;
	    }

	    @Bean
	    public Job inactiveEntryIndexingJob(@Qualifier("inactiveEntryIndexingMainStep") Step inactiveEntryIndexingMainFFStep,
	                                    WriteRetrierLogJobListener writeRetrierLogJobListener) {
	        return this.jobBuilderFactory.get(INACTIVEENTRY_INDEX_JOB)
	                .start(inactiveEntryIndexingMainFFStep)
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

