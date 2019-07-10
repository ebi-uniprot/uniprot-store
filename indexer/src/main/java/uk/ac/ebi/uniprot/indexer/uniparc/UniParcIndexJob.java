package uk.ac.ebi.uniprot.indexer.uniparc;

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
import uk.ac.ebi.uniprot.indexer.common.config.SolrRepositoryConfig;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.indexer.common.listener.WriteRetrierLogJobListener;
import uk.ac.ebi.uniprot.search.SolrCollection;

import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPARC_INDEX_JOB;

/**
 *
 * @author jluo
 * @date: 18 Jun 2019
 *
*/

@Configuration
@Import({SolrRepositoryConfig.class})
public class UniParcIndexJob {
    private final JobBuilderFactory jobBuilderFactory;
    private final UniProtSolrOperations solrOperations;

    @Autowired
    public UniParcIndexJob(JobBuilderFactory jobBuilderFactory, UniProtSolrOperations solrOperations) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.solrOperations = solrOperations;
    }

    @Bean
    public Job uniparcIndexingJob(
            @Qualifier("UniParcIndexStep") Step proteomeIndexStep,

            WriteRetrierLogJobListener writeRetrierLogJobListener) {
        return this.jobBuilderFactory.get(UNIPARC_INDEX_JOB)
                .start(proteomeIndexStep)
                .listener(writeRetrierLogJobListener)
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {
                        // no-op
                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        solrOperations.commit(SolrCollection.uniparc.name());
                    }
                })
                .build();
    }
}
