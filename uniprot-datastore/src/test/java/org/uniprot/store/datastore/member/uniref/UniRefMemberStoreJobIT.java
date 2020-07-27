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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.datastore.utils.Constants.*;

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
                UniRef100MembersStoreStep.class,
                UniRef90MembersStoreStep.class,
                UniRef50MembersStoreStep.class,
                ListenerConfig.class
        })
@EnableConfigurationProperties({UniRefMemberStoreProperties.class})
class UniRefMemberStoreJobIT {
    @Autowired
    private JobLauncherTestUtils jobLauncher;

    @Autowired
    private UniRefMemberStoreProperties unirefMemberStoreProperties;

    @Autowired
    private UniProtStoreClient<RepresentativeMember> unirefMemberStoreClient;

    @Test
    void testUniRefMemberStoreJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(UNIREF_MEMBER_STORE_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(3));

        checkUniRefMemberStoreStep(jobExecution, stepExecutions);
    }

    private void checkUniRefMemberStoreStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions)
            throws IOException {
        verifyUniRef100MemberStoreStep(jobExecution, stepExecutions);
        verifyUniRef90MemberStoreStep(jobExecution, stepExecutions);
        verifyUniRef50MemberStoreStep(jobExecution, stepExecutions);
        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 378);
        verifyVoldemortData();
    }

    private void verifyUniRef100MemberStoreStep(JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution uniref100Step =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF100_MEMBER_STORE_STEP))
                        .findFirst()
                        .get();

        assertThat(uniref100Step.getReadCount(), is(12));
        assertThat(uniref100Step.getWriteCount(), is(12));
    }

    private void verifyUniRef90MemberStoreStep(JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution uniref100Step =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF90_MEMBER_STORE_STEP))
                        .findFirst()
                        .get();

        assertThat(uniref100Step.getReadCount(), is(67));
        assertThat(uniref100Step.getWriteCount(), is(67));

    }

    private void verifyUniRef50MemberStoreStep(JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution uniref100Step =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF50_MEMBER_STORE_STEP))
                        .findFirst()
                        .get();
        assertThat(uniref100Step.getReadCount(), is(378));
        assertThat(uniref100Step.getWriteCount(), is(378));
    }

    private void checkWriteCount(
            JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI =
                (AtomicInteger)
                        jobExecution.getExecutionContext().get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(failedCountAI.get(), CoreMatchers.is(i));
    }

    private void verifyVoldemortData() {
        // check a rep member
        Optional<RepresentativeMember> repMember = unirefMemberStoreClient.getEntry("A0A0H3AR18");
        assertThat(repMember.isPresent(), is(true));
        assertThat(repMember.get().getMemberId(), equalTo("A0A0H3AR18"));
        assertThat(repMember.get().getSequence(), is(notNullValue()));
        assertThat(repMember.get().getMemberIdType(), is(UniRefMemberIdType.UNIPROTKB));
        assertThat(repMember.get().getUniRef100Id().getValue(), equalTo("UniRef100_A0A0H3AR18"));
        assertThat(repMember.get().getUniRef90Id().getValue(), equalTo("UniRef90_A0A0H3AR18"));
        assertThat(repMember.get().getUniRef50Id().getValue(), equalTo("UniRef50_A9W094"));

        // check a member in uniref100, uniref90 and uniref50
        Optional<RepresentativeMember> member100 = unirefMemberStoreClient.getEntry("A0A0E1X2G4");
        assertThat(member100.isPresent(), is(true));
        assertThat(member100.get().getMemberId(), equalTo("A0A0E1X2G4"));
        assertThat(member100.get().getSequence(), is(nullValue()));
        assertThat(member100.get().getMemberIdType(), is(UniRefMemberIdType.UNIPROTKB));
        assertThat(member100.get().getUniRef100Id(), is(notNullValue()));
        assertThat(member100.get().getUniRef90Id(), is(notNullValue()));
        assertThat(member100.get().getUniRef50Id(), is(nullValue()));

        // check a member in uniref90
        Optional<RepresentativeMember> member90 = unirefMemberStoreClient.getEntry("UPI000DD5454A");
        assertThat(member90.isPresent(), is(true));
        assertThat(member90.get().getMemberId(), equalTo("UPI000DD5454A"));
        assertThat(member90.get().getSequence(), is(nullValue()));
        assertThat(member90.get().getMemberIdType(), is(UniRefMemberIdType.UNIPARC));
        assertThat(member90.get().getUniRef100Id(), is(notNullValue()));
        assertThat(member90.get().getUniRef90Id(), is(notNullValue()));
        assertThat(member90.get().getUniRef50Id(), is(nullValue()));

        // check a member in uniref50
        Optional<RepresentativeMember> member50 = unirefMemberStoreClient.getEntry("UPI0004AE23BE");
        assertThat(member50.isPresent(), is(true));
        assertThat(member50.get().getMemberId(), equalTo("UPI0004AE23BE"));
        assertThat(member50.get().getMemberIdType(), is(UniRefMemberIdType.UNIPARC));
        assertThat(member50.get().getSequence(), is(nullValue()));
        assertThat(member50.get().getUniRef100Id(), is(notNullValue()));
        assertThat(member50.get().getUniRef90Id(), is(notNullValue()));
        assertThat(member50.get().getUniRef50Id(), is(nullValue()));
    }
}
