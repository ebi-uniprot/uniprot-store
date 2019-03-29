/*
 * Created by sahmad on 1/31/19 1:51 PM
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.jobs;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.IndexerSpringBootApplication;
import uk.ac.ebi.uniprot.TestConfig;
import uk.ac.ebi.uniprot.api.common.repository.search.SolrDataStoreManager;
import uk.ac.ebi.uniprot.models.DBXRef;
import uk.ac.ebi.uniprot.steps.IndexCrossRefStep;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {IndexerSpringBootApplication.class, TestConfig.class, IndexerJobConfig.class, IndexCrossRefStep.class})
public class IndexerJobConfigTest {

    @Autowired
    private Job indexSupportingData;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private SolrClient solrClient;

    @BeforeAll
    static void setUp() throws IOException {
        SolrDataStoreManager solrDM = new SolrDataStoreManager();
    }

    @Test
    void testIndexerJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time",System.currentTimeMillis()).toJobParameters();

        JobExecution jobExecution = jobLauncher.run(indexSupportingData, jobParameters);
        BatchStatus status = jobExecution.getStatus();
        assertEquals(status, BatchStatus.COMPLETED);

        // get the data and verify
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("q", "*:*");
        QueryResponse response = solrClient.query(params);
        assertEquals(0, response.getStatus());
        List<DBXRef> results = response.getBeans(DBXRef.class);
        assertEquals(10, results.size());
        assertTrue(response.getResults().getNumFound() >= 171);

        // clean up
        solrClient.deleteByQuery("*:*");
        solrClient.commit();

        solrClient.close();
    }
}
