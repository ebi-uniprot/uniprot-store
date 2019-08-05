package org.uniprot.store.datastore.uniprotkb;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.test.FakeStoreSpringBootApplication;
import org.uniprot.store.datastore.uniprotkb.UniProtKBJob;
import org.uniprot.store.datastore.uniprotkb.config.StoreTestConfig;
import org.uniprot.store.datastore.uniprotkb.config.UniProtKBStoreProperties;
import org.uniprot.store.datastore.uniprotkb.step.UniProtKBStep;
import org.uniprot.store.job.common.TestUtils;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.datastore.utils.Constants.UNIPROTKB_STORE_JOB;
import static org.uniprot.store.datastore.utils.Constants.UNIPROTKB_STORE_STEP;

/**
 * Created 28/07/19
 *
 * @author Edd
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {StoreTestConfig.class, TestUtils.class, FakeStoreSpringBootApplication.class, UniProtKBJob.class,
                           UniProtKBStep.class, ListenerConfig.class})
class UniProtKBJobIT {
    @Autowired
    private JobLauncherTestUtils jobLauncher;

    @Autowired
    private UniProtKBStoreProperties uniProtKBStoreProperties;

    @Autowired
    private UniProtStoreClient<UniProtEntry> uniProtKBStoreClient;

    @Test
    void testUniProtKBStoreJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIPROTKB_STORE_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(1));

        checkUniProtKBStoreStep(jobExecution, stepExecutions);
    }

    private void checkUniProtKBStoreStep(JobExecution jobExecution, Collection<StepExecution> stepExecutions)
            throws IOException {
        StepExecution kbStep = stepExecutions.stream()
                .filter(step -> step.getStepName().equals(UNIPROTKB_STORE_STEP))
                .collect(Collectors.toList()).get(0);

        assertThat(kbStep.getReadCount(), is(5));
        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 5);

        // check that the accessions in the source file, are the ones that were written to Solr
        Set<String> sourceAccessions = readSourceAccessions();
        assertThat(sourceAccessions, hasSize(5));

        // check that they all were saved in the store client
        sourceAccessions
                .forEach(acc -> assertThat(uniProtKBStoreClient.getEntry(acc), is(notNullValue())));
    }

    private Set<String> readSourceAccessions() throws IOException {
        return Files.lines(Paths.get(uniProtKBStoreProperties.getUniProtEntryFile()))
                .filter(line -> line.startsWith("AC"))
                .map(line -> line.substring(5, line.length() - 1))
                .collect(Collectors.toSet());
    }

    private void checkWriteCount(JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI = (AtomicInteger) jobExecution.getExecutionContext()
                .get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(failedCountAI.get(), CoreMatchers.is(i));
    }
}