package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.batch.core.BatchStatus;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.listeners.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static uk.ac.ebi.uniprot.indexer.DocumentWriteRetryHelper.*;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_JOB;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@ActiveProfiles(profiles = {"tooManySolrRemoteHostErrors, offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class,
                           UniProtKBJobWithSolrWriteRetriesThenFailureIT.RetryConfig.class,
                           TestConfig.class, UniProtKBJob.class,
                           UniProtKBStep.class, ListenerConfig.class})
class UniProtKBJobWithSolrWriteRetriesThenFailureIT {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private ItemWriter<ConvertibleEntry> uniProtDocumentItemWriter;

    @Captor
    private ArgumentCaptor<List<ConvertibleEntry>> argumentCaptor;

    private static final List<SolrResponse> SOLR_RESPONSES = asList(
            // read first chunk (size 2; read total 2)
            SolrResponse.OK,                // .. then, write first chunk (write total 2)
            // read second chunk (size 2; read total 4)
            SolrResponse.REMOTE_EXCEPTION,  // .. then error when writing (write total 2)
            SolrResponse.OK,                // .. then success when writing (write total 4)
            // read remainder (size 1; read total 5)
            SolrResponse.REMOTE_EXCEPTION,  // .. then error when writing (write total 2)
            SolrResponse.REMOTE_EXCEPTION); // too many errors -- indexing fails

    @Test
    void tooManyRetriesCausesAFailedIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), is(UNIPROTKB_INDEX_JOB));

        List<StepExecution> jobsSingleStepAsList = jobExecution.getStepExecutions()
                .stream()
                .filter(step -> step.getStepName().equals(UNIPROTKB_INDEX_STEP))
                .collect(Collectors.toList());
        assertThat(jobsSingleStepAsList, hasSize(1));

        StepExecution indexingStep = jobsSingleStepAsList.get(0);

        assertThat(indexingStep.getReadCount(), is(5));
        assertThat(indexingStep.getReadSkipCount(), is(0));
        assertThat(indexingStep.getProcessSkipCount(), is(0));
        assertThat(indexingStep.getWriteSkipCount(), is(0));
        assertThat(indexingStep.getWriteCount(), is(4));
        assertThat(indexingStep.getCommitCount(), is(2));

        verify(uniProtDocumentItemWriter, times(5)).write(argumentCaptor.capture());
        List<List<ConvertibleEntry>> docsSentToBeWritten = argumentCaptor.getAllValues();
        validateWriteAttempts(SOLR_RESPONSES, docsSentToBeWritten, d -> d.getDocument().accession);

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.FAILED));
    }

    @Profile("tooManySolrRemoteHostErrors")
    @TestConfiguration
    static class RetryConfig {
        @Bean
        @Primary
        @SuppressWarnings(value = "unchecked")
        ItemWriter<ConvertibleEntry> uniProtDocumentItemWriterMock() throws Exception {
            ItemWriter<ConvertibleEntry> mockItemWriter = mock(ItemWriter.class);

            stubItemWriterWriteResponses(SOLR_RESPONSES)
                    .when(mockItemWriter).write(any());

            return mockItemWriter;
        }
    }
}
