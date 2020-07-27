package org.uniprot.store.datastore.member.uniref;

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
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.member.uniref.config.UniRefMemberStoreProperties;
import org.uniprot.store.datastore.test.FakeStoreSpringBootApplication;
import org.uniprot.store.job.common.TestUtils;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.datastore.utils.Constants.UNIREF_MEMBER_STORE_JOB;
import static org.uniprot.store.datastore.utils.Constants.UNIREF_MEMBER_STORE_STEP;

/**
 * @author sahmad
 * @date: 27 July 2020
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            UniRefMemberStoreTestConfig.class,
            TestUtils.class,
            FakeStoreSpringBootApplication.class,
            UniRefMemberStoreJob.class,
            UniRefMemberStoreStep.class,
            ListenerConfig.class
        })
@EnableConfigurationProperties({UniRefMemberStoreProperties.class})
class UniRefMemberStoreJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniRefMemberStoreProperties unirefMemberStoreProperties;

    @Autowired private UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient;

    @Test
    void testUniRefMemberStoreJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(UNIREF_MEMBER_STORE_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(1));

        checkUniRefMemberStoreStep(jobExecution, stepExecutions);
    }

    private void checkUniRefMemberStoreStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions)
            throws IOException {
        StepExecution kbStep =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF_MEMBER_STORE_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(kbStep.getReadCount(), is(5487));
        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 5487);

        // check a rep member
        Optional<RepresentativeMember> repMember = unirefMemberStoreClient.getEntry("Q9EPS7");
        assertThat(repMember.isPresent(), is(true));
        assertThat(repMember.get().getMemberId(), equalTo("Q9EPS7"));
        assertThat(repMember.get().getSequence(), is(notNullValue()));

        // check a member
        Optional<RepresentativeMember> member = unirefMemberStoreClient.getEntry("UPI0000DBE4A9");
        assertThat(member.isPresent(), is(true));
        assertThat(member.get().getMemberId(), equalTo("UPI0000DBE4A9"));
        assertThat(member.get().getSequence(), is(nullValue()));
        assertThat(member.get().getMemberIdType(), is(UniRefMemberIdType.UNIPARC));
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
