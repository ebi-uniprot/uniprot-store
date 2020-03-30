package org.uniprot.store.datastore.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.datastore.utils.Constants.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.test.FakeStoreSpringBootApplication;
import org.uniprot.store.datastore.uniparc.config.UniParcConfig;
import org.uniprot.store.datastore.uniparc.config.UniParcStoreProperties;
import org.uniprot.store.job.common.TestUtils;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            UniParcConfig.class,
            UniParcStoreTestConfig.class,
            TestUtils.class,
            FakeStoreSpringBootApplication.class,
            UniParcStoreJob.class,
            UniParcStoreStep.class,
            ListenerConfig.class
        })
@EnableConfigurationProperties({UniParcStoreProperties.class})
class UniParcStoreJobIT {

    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtStoreClient<UniParcEntry> uniParcStoreClient;

    @Test
    void testUniParcStoreJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIPARC_STORE_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(1));

        checkUniParcStoreStep(jobExecution, stepExecutions);
    }

    private void checkUniParcStoreStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution kbStep =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIPARC_STORE_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(kbStep.getReadCount(), is(7));
        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 7);

        // check that the accessions in the source file, are the ones that were written to Voldemort
        List<String> ids =
                Arrays.asList(
                        "UPI0005CFF912",
                        "UPI0000578E3B",
                        "UPI0002064FD7",
                        "UPI0000127400",
                        "UPI000046FEFD",
                        "UPI00003779FC",
                        "UPI000387D615");

        ids.forEach(acc -> assertThat(uniParcStoreClient.getEntry(acc), is(notNullValue())));
    }

    private void checkWriteCount(
            JobExecution jobExecution, String uniParcIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI =
                (AtomicInteger)
                        jobExecution.getExecutionContext().get(uniParcIndexFailedEntriesCountKey);
        assertThat(failedCountAI, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(failedCountAI.get(), CoreMatchers.is(i));
    }
}
