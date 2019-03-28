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
import uk.ac.ebi.uniprot.models.DBXRef;
import uk.ac.ebi.uniprot.steps.IndexCrossRefStep;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {IndexerSpringBootApplication.class, TestConfig.class, IndexerJobConfig.class, IndexCrossRefStep.class})
public class IndexerJobConfigTest {
    private static final String SOLR_HOME = "target/test-classes/solr-config/uniprot-collections";
    private static final String DBXREF_COLLECTION_NAME = "crossref";

    @Autowired
    private Job indexSupportingData;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private SolrClient solrClient;

    @BeforeAll
    static void setUp() throws IOException {
        File temporaryFolder = Files.createTempDirectory("solr_data").toFile();
        String solrHomePath = new File(SOLR_HOME).getAbsolutePath();
        System.setProperty("solr.data.dir", temporaryFolder.getAbsolutePath());
        System.setProperty("solr.home",new File(SOLR_HOME).getAbsolutePath());
        System.setProperty("solr.core.name",DBXREF_COLLECTION_NAME);
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

        this.solrClient.close();
    }
}
