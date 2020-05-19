package org.uniprot.store.indexer.unirule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.uniprot.store.indexer.common.utils.Constants.UNIRULE_INDEX_JOB;
import static org.uniprot.store.indexer.common.utils.Constants.UNIRULE_INDEX_STEP;

import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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
import org.uniprot.core.json.parser.unirule.UniRuleJsonConfig;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @date: 14 May 2020
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            UniRuleIndexJob.class,
            UniRuleIndexStep.class,
            ListenerConfig.class
        })
class UniRuleIndexJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;
    @Autowired private UniProtSolrClient solrClient;
    private ObjectMapper objectMapper;

    @BeforeEach
    void cleanUpBefore() {
        // clean up
        cleanUp();
        this.objectMapper = UniRuleJsonConfig.getInstance().getFullObjectMapper();
    }

    @AfterEach
    void cleanUpAfter() {
        // clean up
        cleanUp();
    }

    private void cleanUp() {
        this.solrClient.delete(SolrCollection.unirule, "*:*");
        this.solrClient.commit(SolrCollection.unirule);
    }

    @Test
    void testUniRuleIndexJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIRULE_INDEX_JOB));
        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));
        Collection<StepExecution> steps = jobExecution.getStepExecutions();
        verifySteps(steps);
        // fetch the data from solr and verify
        List<UniRuleDocument> response =
                solrClient.query(
                        SolrCollection.unirule, new SolrQuery("*:*"), UniRuleDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(2));
        assertThat(response.get(0).getUniRuleId(), is("UR001229753"));
        assertThat(response.get(1).getUniRuleId(), is("UR001330252"));
        // verify the rule ids from the serialised object
        response.forEach(this::verifyRule);
    }

    private void verifySteps(Collection<StepExecution> steps) {
        Assertions.assertNotNull(steps);
        assertEquals(1, steps.size());
        StepExecution step = steps.iterator().next();
        assertEquals(UNIRULE_INDEX_STEP, step.getStepName());
        assertEquals(BatchStatus.COMPLETED, step.getStatus());
        assertEquals(2, step.getReadCount());
        assertEquals(2, step.getWriteCount());
        assertEquals(0, step.getReadSkipCount());
        assertEquals(0, step.getWriteSkipCount());
        assertEquals(0, step.getProcessSkipCount());
        assertEquals(0, step.getRollbackCount());
    }

    private void verifyRule(UniRuleDocument uniRuleDocument) {
        String uniRuleId = uniRuleDocument.getUniRuleId();
        byte[] obj = uniRuleDocument.getUniRuleObj().array();
        try {
            UniRuleEntry uniRuleEntry = objectMapper.readValue(obj, UniRuleEntry.class);
            assertEquals(uniRuleId, uniRuleEntry.getUniRuleId().getValue());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
