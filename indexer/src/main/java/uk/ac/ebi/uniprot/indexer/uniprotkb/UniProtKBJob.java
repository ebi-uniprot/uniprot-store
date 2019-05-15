package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogJobListener;
import uk.ac.ebi.uniprot.search.SolrCollection;

import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_JOB;

/**
 * The main UniProtKB indexing job.
 *
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
public class UniProtKBJob {
    private final JobBuilderFactory jobBuilderFactory;
    private final SolrTemplate solrTemplate;

    @Autowired
    public UniProtKBJob(JobBuilderFactory jobBuilderFactory, SolrTemplate solrTemplate) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.solrTemplate = solrTemplate;
    }

    @Bean
    public Job uniProtKBIndexingJob(@Qualifier("xxxx") Step uniProtKBIndexingMainFFStep,
                                    @Qualifier("yyyy") Step suggestionStep,
                                    WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory.get(UNIPROTKB_INDEX_JOB)
                .start(uniProtKBIndexingMainFFStep)
                .next(suggestionStep)
                .listener(writeRetrierLogJobListener)
                .listener(new JobExecutionListener() {
                    @Override public void beforeJob(JobExecution jobExecution) {
                        // no-op
                    }

                    // Hard commit contents of repository once job has finished.
                    // Delegate all other commits to 'autoCommit' element of solrconfig.xml
                    @Override public void afterJob(JobExecution jobExecution) {
                        solrTemplate.commit(SolrCollection.uniprot.name());
                    }
                })
                .build();
    }
}
