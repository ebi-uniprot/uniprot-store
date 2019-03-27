/*
 * Created by sahmad on 1/31/19 1:51 PM
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.jobs;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.IndexerSpringBootApplication;
import uk.ac.ebi.uniprot.TestConfig;
import uk.ac.ebi.uniprot.steps.IndexCrossRefStep;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {IndexerSpringBootApplication.class, TestConfig.class, IndexerJobConfig.class, IndexCrossRefStep.class})
public class IndexerJobConfigTest {
    @Autowired
    private Job indexSupportingData;
    @Autowired
    private JobLauncher jobLauncher;

    @Test
    void testIndexerJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time",System.currentTimeMillis()).toJobParameters();

        JobExecution jobExecution = jobLauncher.run(indexSupportingData, jobParameters);
        BatchStatus status = jobExecution.getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }
}
