package org.uniprot.store.indexer.uniprotkb;

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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import org.uniprot.store.indexer.uniprotkb.step.SuggestionStep;
import org.uniprot.store.indexer.uniprotkb.step.UniProtKBStep;
import org.uniprot.store.indexer.uniprotkb.writer.UniProtEntryDocumentPairWriter;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;
import org.uniprot.store.search.SolrCollection;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.uniprot.store.indexer.DocumentWriteRetryHelper.SolrResponse;
import static org.uniprot.store.indexer.DocumentWriteRetryHelper.stubSolrWriteResponses;
import static org.uniprot.store.indexer.common.utils.Constants.UNIPROTKB_INDEX_JOB;
import static org.uniprot.store.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@ActiveProfiles(profiles = {"manySolrRemoteHostErrors, offline", "job"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class,
                           UniProtKBJobWriteSolrRetriesToLogFileThenFailIT.RetryConfig.class,
                           SolrTestConfig.class, UniProtKBJob.class,
                           UniProtKBStep.class, SuggestionStep.class, ListenerConfig.class})
@TestPropertySource(properties = {"uniprotkb.indexing.itemProcessorTaskExecutor.corePoolSize=1",
                                  "uniprotkb.indexing.itemProcessorTaskExecutor.maxPoolSize=1",
                                  "uniprotkb.indexing.itemWriterTaskExecutor.corePoolSize=1",
                                  "uniprotkb.indexing.itemWriterTaskExecutor.maxPoolSize=1"}
)
class UniProtKBJobWriteSolrRetriesToLogFileThenFailIT {
    private static final String INDEXING_DOC_WRITE_FAILED_ENTRIES_LOG = "store-write-failed-entries.error";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private static final List<SolrResponse> SOLR_RESPONSES = asList(
            SolrResponse.REMOTE_EXCEPTION,  // .. chunk 1 failed to write
            SolrResponse.REMOTE_EXCEPTION,  // .. chunk 1 failed to write
            SolrResponse.REMOTE_EXCEPTION,  // .. chunk 2 failed to write
            SolrResponse.OK,                // .. chunk 2 written
            SolrResponse.REMOTE_EXCEPTION,  // .. chunk 3 failed to write
            SolrResponse.REMOTE_EXCEPTION); // .. chunk 3 failed to write

    @Test
//    @Disabled
    void tooManyRetriesCauseAFailedIndexingJob() throws Exception {
        Path logFileForErrors = truncateErrorLogFileBeforeTest();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), is(UNIPROTKB_INDEX_JOB));

        List<StepExecution> jobsSingleStepAsList = jobExecution.getStepExecutions()
                .stream()
                .filter(step -> step.getStepName().equals(UNIPROTKB_INDEX_STEP))
                .collect(Collectors.toList());
        assertThat(jobsSingleStepAsList, hasSize(1));

        StepExecution indexingStep = jobsSingleStepAsList.get(0);

        assertThat(indexingStep.getReadCount(), is(5));  // ensure everything was read

        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 3);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 2);

        assertThat(indexingStep.getExitStatus(), is(ExitStatus.FAILED));
        assertThat(indexingStep.getStatus(), is(BatchStatus.FAILED));
        assertThat(jobExecution.getExitStatus(), is(ExitStatus.FAILED));
        assertThat(jobExecution.getStatus(), is(BatchStatus.FAILED));

        checkErrorFileCreated(logFileForErrors);
    }

    private Path truncateErrorLogFileBeforeTest() throws FileNotFoundException {
        String logFileNameForErrors = INDEXING_DOC_WRITE_FAILED_ENTRIES_LOG;
        Path logFileForErrors = Paths.get(logFileNameForErrors);
        // truncate any previous log file used to store entry write errors
        // so that we can check for new content later
        if (Files.exists(logFileForErrors)) {
            PrintWriter fileWriter = new PrintWriter(logFileNameForErrors);
            fileWriter.print("");
            fileWriter.close();
        }
        return logFileForErrors;
    }

    private void checkErrorFileCreated(Path logFileForErrors) throws IOException, InterruptedException {
        // wait for the file to be written
        Thread.sleep(500);

        // THEN --------------------------------
        // ensure this entry is written to the error log
        assertTrue(Files.exists(logFileForErrors));

        // sanity check: ensure the error log contains the correct number of accessions
        Stream<String> lines = Files.lines(logFileForErrors);
        int accessionCountInErrorFile = lines
                .filter(l -> l.startsWith("AC   "))
                .collect(Collectors.toList())
                .size();
        assertThat(accessionCountInErrorFile, is(3));
    }

    private void checkWriteCount(JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI = (AtomicInteger) jobExecution.getExecutionContext()
                .get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, is(notNullValue()));
        assertThat(failedCountAI.get(), is(i));
    }

    @Profile("manySolrRemoteHostErrors")
    @TestConfiguration
    static class RetryConfig {

        @Bean
        @Primary
        @SuppressWarnings(value = "unchecked")
        ItemWriter<UniProtEntryDocumentPair> uniProtDocumentItemWriterMock() {
            UniProtSolrOperations mockSolrTemplate = mock(UniProtSolrOperations.class);
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
