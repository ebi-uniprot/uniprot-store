package org.uniprot.store.indexer.proteome;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.uniprot.store.indexer.common.utils.Constants.PROTEOME_INDEX_JOB;
import static org.uniprot.store.indexer.common.utils.Constants.SUGGESTIONS_INDEX_STEP;

import java.util.Collection;
import java.util.List;
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
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author jluo
 * @date: 25 Apr 2019
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            ProteomeIndexJob.class,
            ProteomeIndexStep.class,
            ProteomeConfig.class,
            ListenerConfig.class,
            ProteomeSuggestionStep.class
        })
class ProteomeIndexIT {
    @Autowired private JobLauncherTestUtils jobLauncher;
    @Autowired private UniProtSolrClient solrOperations;
    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testIndexJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(PROTEOME_INDEX_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        List<ProteomeDocument> response =
                solrOperations.query(
                        SolrCollection.proteome, new SolrQuery("*:*"), ProteomeDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(6));

        response.forEach(val -> verifyProteome(val));

        checkSuggestionIndexingStep(jobExecution.getStepExecutions());
    }

    private void verifyProteome(ProteomeDocument doc) {
        String upid = doc.upid;
        ObjectMapper objectMapper = ProteomeJsonConfig.getInstance().getFullObjectMapper();
        byte[] obj = doc.proteomeStored.array();
        try {
            ProteomeEntry proteome = objectMapper.readValue(obj, ProteomeEntry.class);
            assertEquals(upid, proteome.getId().getValue());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void checkSuggestionIndexingStep(Collection<StepExecution> stepExecutions) {
        StepExecution suggestionIndexingStep =
                stepExecutions.stream()
                        .filter(step -> step.getStepName().equals(SUGGESTIONS_INDEX_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(suggestionIndexingStep.getReadCount(), CoreMatchers.is(greaterThan(0)));

        int reportedWriteCount = suggestionIndexingStep.getWriteCount();
        assertThat(reportedWriteCount, CoreMatchers.is(greaterThan(0)));
        assertThat(suggestionIndexingStep.getSkipCount(), CoreMatchers.is(0));
        assertThat(suggestionIndexingStep.getFailureExceptions(), hasSize(0));

        List<SuggestDocument> response =
                solrClient.query(
                        SolrCollection.suggest,
                        new SolrQuery("*:*").setRows(300),
                        SuggestDocument.class);
        assertThat(response, CoreMatchers.is(CoreMatchers.notNullValue()));
        assertThat(response.size(), CoreMatchers.is(reportedWriteCount));
    }
}
