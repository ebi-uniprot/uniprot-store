package org.uniprot.store.indexer.arba;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.indexer.common.utils.Constants.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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
import org.uniprot.store.indexer.test.config.FakeReadDatabaseConfig;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.arba.ArbaDocument;

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
            FakeReadDatabaseConfig.class,
            SolrTestConfig.class,
            ArbaProteinCountStep.class,
            ArbaIndexStep.class,
            ArbaIndexJob.class,
            ListenerConfig.class
        })
class ArbaIndexJobIT {
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
        this.solrClient.delete(SolrCollection.arba, "*:*");
        this.solrClient.commit(SolrCollection.arba);
    }

    @Test
    void testArbaIndexJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(ARBA_INDEX_JOB));
        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));
        Collection<StepExecution> steps = jobExecution.getStepExecutions();
        verifySteps(steps);
        // fetch the data from solr and verify
        List<ArbaDocument> response =
                solrClient.query(SolrCollection.arba, new SolrQuery("*:*"), ArbaDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(5));
        assertThat(response.stream().map(ArbaDocument::getRuleId).collect(Collectors.toList()),
                contains("ARBA00000001", "ARBA00023492", "ARBA00023910", "ARBA00000002", "ARBA00000003"));
        // verify the rule ids from the serialised object
        response.forEach(this::verifyRule);
    }

    private void verifySteps(Collection<StepExecution> steps) {
        Assertions.assertNotNull(steps);
        assertEquals(2, steps.size());
        Iterator<StepExecution> iter = steps.iterator();
        StepExecution step1 = iter.next();
        StepExecution step2 = iter.next();
        assertEquals(ARBA_PROTEIN_COUNT_STEP, step1.getStepName());
        assertEquals(BatchStatus.COMPLETED, step2.getStatus());
        assertEquals(ARBA_INDEX_STEP, step2.getStepName());
        assertEquals(BatchStatus.COMPLETED, step2.getStatus());
    }

    private void verifyRule(ArbaDocument uniRuleDocument) {
        String ruleId = uniRuleDocument.getRuleId();
        byte[] obj = uniRuleDocument.getRuleObj().array();
        try {
            UniRuleEntry uniRuleEntry = objectMapper.readValue(obj, UniRuleEntry.class);
            assertEquals(ruleId, uniRuleEntry.getUniRuleId().getValue());
            assertTrue(uniRuleEntry.getProteinsAnnotatedCount() > 0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
