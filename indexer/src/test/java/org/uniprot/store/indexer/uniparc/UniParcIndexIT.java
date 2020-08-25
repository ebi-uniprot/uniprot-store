package org.uniprot.store.indexer.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.uniprot.store.indexer.common.utils.Constants.SUGGESTIONS_INDEX_STEP;
import static org.uniprot.store.indexer.common.utils.Constants.UNIPARC_INDEX_JOB;

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
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * @author jluo
 * @date: 18 Jun 2019
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            UniParcIndexJob.class,
            UniParcIndexStep.class,
            ListenerConfig.class,
            UniParcTaxonomySuggestionStep.class
        })
class UniParcIndexIT {
    @Autowired private JobLauncherTestUtils jobLauncher;
    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testIndexJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIPARC_INDEX_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        List<UniParcDocument> response =
                solrClient.query(
                        SolrCollection.uniparc, new SolrQuery("*:*"), UniParcDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(3));
        assertThat(response.get(0).getUpi(), is("UPI0000127191"));
        assertThat(response.get(1).getUpi(), is("UPI0000127192"));
        assertThat(response.get(2).getUpi(), is("UPI0000127193"));

        checkSuggestionIndexingStep(jobExecution.getStepExecutions());
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
