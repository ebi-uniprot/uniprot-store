package org.uniprot.store.indexer.uniref;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.uniprot.store.indexer.common.utils.Constants.UNIREF_INDEX_JOB;

import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
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
import org.uniprot.store.search.document.uniref.UniRefDocument;

/**
 * @author jluo
 * @date: 15 Aug 2019
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            UniRefIndexJob.class,
            UniRefIndexStep.class,
            ListenerConfig.class
        })
class UniRefIndexIT {
    @Autowired private JobLauncherTestUtils jobLauncher;
    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testIndexJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(UNIREF_INDEX_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        List<UniRefDocument> response =
                solrClient.query(SolrCollection.uniref, new SolrQuery("*:*"), UniRefDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(2));
        assertThat(response.get(0).getId(), is("UniRef50_Q9EPS7"));
        assertThat(response.get(1).getId(), is("UniRef50_Q95604"));
    }
}
