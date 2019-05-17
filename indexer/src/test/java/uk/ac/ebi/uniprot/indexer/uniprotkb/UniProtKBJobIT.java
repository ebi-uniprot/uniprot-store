package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.indexer.uniprotkb.step.SuggestionStep;
import uk.ac.ebi.uniprot.indexer.uniprotkb.step.UniProtKBStep;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.*;

/**
 * Created 11/04/19
 *
 * @author Edd
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class, UniProtKBJob.class,
                           UniProtKBStep.class, SuggestionStep.class, ListenerConfig.class})
@TestPropertySource(properties = "spring.batch.job.enabled=false")
class UniProtKBJobIT {
    @Autowired
    private JobLauncherTestUtils jobLauncher;
    @Autowired
    private SolrTemplate template;

    @Test
    void testUniProtKBIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIPROTKB_INDEX_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(2));

        checkUniProtKBIndexingStep(jobExecution, stepExecutions);
        checkSuggestionIndexingStep(stepExecutions);
    }

    private void checkSuggestionIndexingStep(Collection<StepExecution> stepExecutions) {
        StepExecution suggestionIndexingStep = stepExecutions.stream()
                .filter(step -> step.getStepName().equals(SUGGESTIONS_INDEX_STEP))
                .collect(Collectors.toList()).get(0);

        assertThat(suggestionIndexingStep.getReadCount(), is(greaterThan(0)));

        int reportedWriteCount = suggestionIndexingStep.getWriteCount();
        assertThat(reportedWriteCount, is(greaterThan(0)));
        assertThat(suggestionIndexingStep.getSkipCount(), is(0));
        assertThat(suggestionIndexingStep.getFailureExceptions(), hasSize(0));


        Page<SuggestDocument> response = template
                .query(SolrCollection.suggest.name(), new SimpleQuery("*:*"), SuggestDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is((long)reportedWriteCount));
    }

    private void checkUniProtKBIndexingStep(JobExecution jobExecution, Collection<StepExecution> stepExecutions) {
        StepExecution kbIndexingStep = stepExecutions.stream()
                .filter(step -> step.getStepName().equals(UNIPROTKB_INDEX_STEP))
                .collect(Collectors.toList()).get(0);

        assertThat(kbIndexingStep.getReadCount(), is(5));
        checkWriteCount(jobExecution, Constants.INDEX_FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, Constants.INDEX_WRITTEN_ENTRIES_COUNT_KEY, 5);

        Page<UniProtDocument> response = template
                .query(SolrCollection.uniprot.name(), new SimpleQuery("*:*"), UniProtDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(5L));

    }

    private void checkWriteCount(JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI = (AtomicInteger) jobExecution.getExecutionContext()
                .get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(failedCountAI.get(), CoreMatchers.is(i));
    }
}