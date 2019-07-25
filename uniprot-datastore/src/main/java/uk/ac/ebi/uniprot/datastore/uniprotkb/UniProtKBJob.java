package uk.ac.ebi.uniprot.datastore.uniprotkb;

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
import uk.ac.ebi.uniprot.datastore.listener.WriteRetrierLogJobListener;
import uk.ac.ebi.uniprot.indexer.common.config.CacheConfig;
import uk.ac.ebi.uniprot.indexer.common.config.SolrRepositoryConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;

import static uk.ac.ebi.uniprot.datastore.utils.Constants.UNIPROTKB_DATASTORE_JOB;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_JOB;

// TODO: 25/07/19 refactor *repos into common module
// TODO: 25/07/19 refactor UUWstoreclient here, and call it uniprotstoreclient
// TODO: 25/07/19 rename uniprot -> uniprotkb, for this
// TODO: 25/07/19 rename datastore -> store

/**
 * The main UniProtKB data storing job.
 * <p>
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@Import({SolrRepositoryConfig.class, CacheConfig.class})
public class UniProtKBJob {
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public UniProtKBJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Job uniProtKBIndexingJob(@Qualifier("uniProtKBDataStoreMainStep") Step uniProtKBDataStoreMainStep,
                                    WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory.get(UNIPROTKB_DATASTORE_JOB)
                .start(uniProtKBDataStoreMainStep)
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
