package uk.ac.ebi.uniprot.indexer.proteome;

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
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.config.SolrRepositoryConfig;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogJobListener;
import uk.ac.ebi.uniprot.search.SolrCollection;

import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.PROTEOME_INDEX_JOB;

/**
 *
 * @author jluo
 * @date: 18 Apr 2019
 *
*/
@Configuration
@Import({SolrRepositoryConfig.class})
public class ProteomeIndexJob {
	 private final JobBuilderFactory jobBuilderFactory;
	    private final SolrTemplate solrTemplate;

	    @Autowired
	    public ProteomeIndexJob(JobBuilderFactory jobBuilderFactory, SolrTemplate solrTemplate) {
	        this.jobBuilderFactory = jobBuilderFactory;
	        this.solrTemplate = solrTemplate;
	    }
	    @Bean
	    public Job ProteomeIndexingJob(
	    		@Qualifier("ProteomeIndexStep") Step proteomeIndexStep,
	    		
	    		WriteRetrierLogJobListener writeRetrierLogJobListener) {
	        return this.jobBuilderFactory.get(PROTEOME_INDEX_JOB)
	                .start(proteomeIndexStep)
	                .listener(writeRetrierLogJobListener)
	                .listener(new JobExecutionListener() {
	                    @Override public void beforeJob(JobExecution jobExecution) {
	                        // no-op
	                    }

	                    @Override public void afterJob(JobExecution jobExecution) {
	                        solrTemplate.commit(SolrCollection.proteome.name());
	                    }
	                })
	                .build();
	    }
}

