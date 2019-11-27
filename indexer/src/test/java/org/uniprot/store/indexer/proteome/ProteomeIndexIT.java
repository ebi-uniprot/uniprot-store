package org.uniprot.store.indexer.proteome;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.uniprot.store.indexer.common.utils.Constants.PROTEOME_INDEX_JOB;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

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
            ListenerConfig.class
        })
class ProteomeIndexIT {
    @Autowired private JobLauncherTestUtils jobLauncher;
    @Autowired private UniProtSolrOperations solrOperations;

    @Test
    void testIndexJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(PROTEOME_INDEX_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Page<ProteomeDocument> response =
                solrOperations.query(
                        SolrCollection.proteome.name(),
                        new SimpleQuery("*:*"),
                        ProteomeDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(6l));

        response.forEach(val -> verifyProteome(val));

        Page<GeneCentricDocument> response2 =
                solrOperations.query(
                        SolrCollection.genecentric.name(),
                        new SimpleQuery("*:*"),
                        GeneCentricDocument.class);
        assertThat(response2, is(notNullValue()));
        assertThat(response2.getTotalElements(), is(2192L));
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
}
