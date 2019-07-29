package uk.ac.ebi.uniprot.datastore.uniprotkb;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.job.common.listener.WriteRetrierLogJobListener;

import static uk.ac.ebi.uniprot.datastore.utils.Constants.UNIPROTKB_STORE_JOB;

/**
 * The main UniProtKB data storing job.
 * <p>
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
public class UniProtKBJob {
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public UniProtKBJob(JobBuilderFactory jobBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Job uniProtKBIndexingJob(@Qualifier("uniProtKBStoreMainStep") Step uniProtKBDataStoreMainStep,
                                    WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory.get(UNIPROTKB_STORE_JOB)
                .start(uniProtKBDataStoreMainStep)
                .listener(writeRetrierLogJobListener)
                .build();
    }
}
