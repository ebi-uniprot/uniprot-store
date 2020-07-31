package org.uniprot.store.datastore.member.uniref;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.datastore.utils.Constants.*;
import static org.uniprot.store.datastore.voldemort.member.uniref.VoldemortInMemoryUniRefMemberStore.getMemberId;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

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

    private void verifyUniRef100MemberStoreStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution uniref100Step =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF100_MEMBER_STORE_STEP))
                        .findFirst()
                        .get();

        assertThat(uniref100Step.getReadCount(), is(12));
        assertThat(uniref100Step.getWriteCount(), is(12));
    }

    private void verifyUniRef90MemberStoreStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution uniref90Step =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF90_MEMBER_STORE_STEP))
                        .findFirst()
                        .get();

        assertThat(getReadCount(uniref90Step), is(67));
        assertThat(getWriteCount(uniref90Step), is(67));
    }

    private void verifyUniRef50MemberStoreStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution uniref50Step =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIREF50_MEMBER_STORE_STEP))
                        .findFirst()
                        .get();
        assertThat(getReadCount(uniref50Step), is(378));
        assertThat(getWriteCount(uniref50Step), is(378));
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
        assertThat(getMemberId(repMember.get()), equalTo("A0A0H3AR18"));
        assertThat(repMember.get().getMemberId(), equalTo("A0A0H3AR18_BRUO2"));
        assertThat(repMember.get().getSequence(), is(notNullValue()));
        assertThat(repMember.get().getMemberIdType(), is(UniRefMemberIdType.UNIPROTKB));
        assertThat(repMember.get().getUniRef100Id().getValue(), equalTo("UniRef100_A0A0H3AR18"));
        assertThat(repMember.get().getUniRef90Id().getValue(), equalTo("UniRef90_A0A0H3AR18"));
        assertThat(repMember.get().getUniRef50Id().getValue(), equalTo("UniRef50_A9W094"));

        // check a member in uniref100, uniref90 and uniref50
        Optional<RepresentativeMember> member100 = unirefMemberStoreClient.getEntry("A0A0E1X2G4");
        assertThat(member100.isPresent(), is(true));
        assertThat(member100.get().getMemberId(), equalTo("A0A0E1X2G4_9RHIZ"));
        assertThat(getMemberId(member100.get()), equalTo("A0A0E1X2G4"));
        assertThat(member100.get().getSequence(), is(nullValue()));
        assertThat(member100.get().getMemberIdType(), is(UniRefMemberIdType.UNIPROTKB));
        assertThat(member100.get().getUniRef100Id(), is(notNullValue()));
        assertThat(member100.get().getUniRef90Id(), is(notNullValue()));
        assertThat(member100.get().getUniRef50Id(), is(nullValue()));

        // check a uniparc member in uniref50 and uniref 90 member
        Optional<RepresentativeMember> member9050 =
                unirefMemberStoreClient.getEntry("UPI000288BB9F");
        assertThat(member9050.isPresent(), is(true));
        assertThat(member9050.get().getMemberId(), equalTo("UPI000288BB9F"));
        assertThat(member9050.get().getMemberIdType(), is(UniRefMemberIdType.UNIPARC));
        assertThat(member9050.get().getSequence(), is(nullValue()));
        assertThat(member9050.get().getUniRef100Id(), is(notNullValue()));
        assertThat(member9050.get().getUniRef90Id(), is(notNullValue()));
        assertThat(member9050.get().getUniRef50Id(), is(nullValue()));
        assertThat(member9050.get().getProteinName(), equalTo("endonuclease III"));
        assertThat(member9050.get().getOrganismName(), equalTo("Ochrobactrum"));
        assertThat(member9050.get().getOrganismTaxId(), equalTo(528l));
        assertThat(member9050.get().getSequenceLength(), equalTo(249));
    }

    private int getReadCount(StepExecution stepExecution) {
        AtomicInteger readAtomic =
                (AtomicInteger)
                        stepExecution
                                .getExecutionContext()
                                .get(CommonConstants.READ_ENTRIES_COUNT_KEY);
        return readAtomic.get();
    }

    private int getWriteCount(StepExecution stepExecution) {
        AtomicInteger writeAtomic =
                (AtomicInteger)
                        stepExecution
                                .getExecutionContext()
                                .get(CommonConstants.WRITTEN_ENTRIES_COUNT_KEY);
        return writeAtomic.get();
    }
}
