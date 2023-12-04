package org.uniprot.store.indexer.uniprotkb;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.uniprot.store.indexer.common.utils.Constants.SUGGESTIONS_INDEX_STEP;
import static org.uniprot.store.indexer.common.utils.Constants.UNIPROTKB_INDEX_JOB;
import static org.uniprot.store.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import org.uniprot.store.indexer.uniprotkb.step.SuggestionStep;
import org.uniprot.store.indexer.uniprotkb.step.UniProtKBStep;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * Created 11/04/19
 *
 * @author Edd
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            SolrTestConfig.class, FakeIndexerSpringBootApplication.class, UniProtKBJob.class,
            UniProtKBStep.class, SuggestionStep.class, ListenerConfig.class
        })
@TestPropertySource(
        properties = {
            "uniprotkb.indexing.itemProcessorTaskExecutor.corePoolSize=1",
            "uniprotkb.indexing.itemProcessorTaskExecutor.maxPoolSize=1",
            "uniprotkb.indexing.itemWriterTaskExecutor.corePoolSize=1",
            "uniprotkb.indexing.itemWriterTaskExecutor.maxPoolSize=1"
        })
class UniProtKBJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;
    @Autowired private UniProtSolrClient solrClient;
    @Autowired private UniProtKBIndexingProperties indexingProperties;

    @Test
    void testUniProtKBIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIPROTKB_INDEX_JOB));

        //        Thread.sleep(10000);

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(2));

        checkUniProtKBIndexingStep(jobExecution, stepExecutions);
        checkSuggestionIndexingStep(stepExecutions);
    }

    private void checkUniProtKBIndexingStep(
            JobExecution jobExecution, Collection<StepExecution> stepExecutions)
            throws IOException {
        StepExecution kbIndexingStep =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(UNIPROTKB_INDEX_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(kbIndexingStep.getReadCount(), is(5));
        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 5);

        // check that the accessions in the source file, are the ones that were written to Solr
        Set<String> sourceAccessions = readSourceAccessions();
        assertThat(sourceAccessions, hasSize(5));

        List<UniProtDocument> response =
                solrClient.query(
                        SolrCollection.uniprot, new SolrQuery("*:*"), UniProtDocument.class);

        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(5));

        assertThat(
                response.stream().map(doc -> doc.accession).collect(Collectors.toSet()),
                is(sourceAccessions));
    }

    private void checkSuggestionIndexingStep(Collection<StepExecution> stepExecutions) {
        StepExecution suggestionIndexingStep =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(SUGGESTIONS_INDEX_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(suggestionIndexingStep.getReadCount(), is(greaterThan(0)));

        int reportedWriteCount = suggestionIndexingStep.getWriteCount();
        assertThat(reportedWriteCount, is(greaterThan(0)));
        assertThat(suggestionIndexingStep.getSkipCount(), is(0));
        assertThat(suggestionIndexingStep.getFailureExceptions(), hasSize(0));

        List<SuggestDocument> response =
                solrClient.query(
                        SolrCollection.suggest,
                        new SolrQuery("*:*").setRows(400),
                        SuggestDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(91));
    }

    private Set<String> readSourceAccessions() throws IOException {
        return Files.lines(Paths.get(indexingProperties.getUniProtEntryFile()))
                .filter(line -> line.startsWith("AC"))
                .map(line -> line.substring(5, line.length() - 1))
                .collect(Collectors.toSet());
    }

    private void checkWriteCount(
            JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI =
                (AtomicInteger)
                        jobExecution.getExecutionContext().get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, CoreMatchers.is(notNullValue()));
        assertThat(failedCountAI.get(), CoreMatchers.is(i));
    }
}
