package uk.ac.ebi.uniprot.indexer.uniprotkb;

import net.jodah.failsafe.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.model.EntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.indexer.uniprotkb.step.UniProtKBStep;
import uk.ac.ebi.uniprot.indexer.uniprotkb.writer.UniProtEntryDocumentPairWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static uk.ac.ebi.uniprot.indexer.DocumentWriteRetryHelper.SolrResponse;
import static uk.ac.ebi.uniprot.indexer.DocumentWriteRetryHelper.stubSolrWriteResponses;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_JOB;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@ActiveProfiles(profiles = {"notTooManySolrRemoteHostErrors", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class,
                           UniProtKBJobWithSolrWriteRetriesThenSuccessIT.RetryConfig.class,
                           TestConfig.class, UniProtKBJob.class,
                           UniProtKBStep.class, ListenerConfig.class})
class UniProtKBJobWithSolrWriteRetriesThenSuccessIT {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private static final List<SolrResponse> SOLR_RESPONSES = asList(
                                            // read first chunk (size 2; read total 2)
            SolrResponse.OK,                // .. then, write first chunk (write total 2)
                                            // read second chunk (size 2; read total 4)
            SolrResponse.REMOTE_EXCEPTION,  // .. then error when writing (write total 2)
            SolrResponse.OK,                // .. then success when writing (write total 4)
            SolrResponse.OK);               // all written

    @Test
    void notTooManyRetriesCauseASuccessfulIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), is(UNIPROTKB_INDEX_JOB));

        List<StepExecution> jobsSingleStepAsList = jobExecution.getStepExecutions()
                .stream()
                .filter(step -> step.getStepName().equals(UNIPROTKB_INDEX_STEP))
                .collect(Collectors.toList());
        assertThat(jobsSingleStepAsList, hasSize(1));

        StepExecution indexingStep = jobsSingleStepAsList.get(0);

        assertThat(indexingStep.getReadCount(), is(5));  // ensure everything was read

        checkWriteCount(jobExecution, Constants.INDEX_FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, Constants.INDEX_WRITTEN_ENTRIES_COUNT_KEY, 5);

        assertThat(indexingStep.getExitStatus(), is(ExitStatus.COMPLETED));
        assertThat(indexingStep.getStatus(), is(BatchStatus.COMPLETED));
        assertThat(jobExecution.getExitStatus(), is(ExitStatus.COMPLETED));
        assertThat(jobExecution.getStatus(), is(BatchStatus.COMPLETED));
    }

    private void checkWriteCount(JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI = (AtomicInteger) jobExecution.getExecutionContext()
                .get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, is(notNullValue()));
        assertThat(failedCountAI.get(), is(i));
    }

    @Profile("notTooManySolrRemoteHostErrors")
    @TestConfiguration
    static class RetryConfig {
        @Bean
        @Primary
        @SuppressWarnings(value = "unchecked")
        ItemWriter<EntryDocumentPair<UniProtEntry, UniProtDocument>> uniProtDocumentItemWriterIT() throws Exception {
            SolrTemplate mockSolrTemplate = mock(SolrTemplate.class);
            stubSolrWriteResponses(SOLR_RESPONSES).when(mockSolrTemplate)
                    .saveBeans(eq(SolrCollection.uniprot.name()), any());

            return new UniProtEntryDocumentPairWriter(mockSolrTemplate,
                                                      SolrCollection.uniprot,
                                                      new RetryPolicy<>()
                                                      .withMaxRetries(2)
                                                      .withBackoff(1, 2, ChronoUnit.SECONDS));
        }
    }
}
