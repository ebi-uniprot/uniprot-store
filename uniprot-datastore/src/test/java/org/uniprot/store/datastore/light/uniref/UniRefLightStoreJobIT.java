package org.uniprot.store.datastore.light.uniref;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.datastore.utils.Constants.*;

import java.io.IOException;
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
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.light.uniref.config.UniRefLightStoreProperties;
import org.uniprot.store.datastore.test.FakeStoreSpringBootApplication;
import org.uniprot.store.job.common.TestUtils;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;

/**
 * @author jluo
 * @date: 20 Aug 2019
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            UniRefLightStoreTestConfig.class,
            TestUtils.class,
            FakeStoreSpringBootApplication.class,
            UniRefLightStoreJob.class,
            UniRefLightStoreStep.class,
            ListenerConfig.class
        })
@EnableConfigurationProperties({UniRefLightStoreProperties.class})
class UniRefLightStoreJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniRefLightStoreProperties unirefLightStoreProperties;

    @Autowired private UniProtStoreClient<UniRefEntryLight> unirefLightStoreClient;

    @Test
    void testUniRefLightStoreJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(UNIREF_LIGHT_STORE_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(1));

        checkUniRefLightStoreStep(jobExecution, stepExecutions);
    }

    private void checkUniRefLightStoreStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions)
            throws IOException {
        StepExecution kbStep =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF_LIGHT_STORE_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(kbStep.getReadCount(), is(2));
        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 2);

        // check that the accessions in the source file, are the ones that were written to dataStore
        List<String> ids = Arrays.asList("UniRef50_Q9EPS7", "UniRef50_Q95604");

        ids.forEach(acc -> assertThat(unirefLightStoreClient.getEntry(acc), is(notNullValue())));
    }

    private void checkWriteCount(
            JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI =
                (AtomicInteger)
                        jobExecution.getExecutionContext().get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(failedCountAI.get(), CoreMatchers.is(i));
    }
}
