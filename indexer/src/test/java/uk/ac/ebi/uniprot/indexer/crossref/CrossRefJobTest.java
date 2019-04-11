package uk.ac.ebi.uniprot.indexer.crossref;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.config.DataSourceConfig;
import uk.ac.ebi.uniprot.indexer.common.listeners.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.crossref.steps.CrossRefStep;
import uk.ac.ebi.uniprot.indexer.crossref.steps.CrossRefUniProtCountStep;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.search.document.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import static org.junit.jupiter.api.Assertions.*;
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, DataSourceConfig.class,
        TestConfig.class, CrossRefJob.class, CrossRefStep.class, CrossRefUniProtCountStep.class, ListenerConfig.class})
class CrossRefJobTest {

    @Autowired
    private Job indexCrossRefJob;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private SolrTemplate template;

    @Disabled
    @Test
    void testIndexerJob() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time",System.currentTimeMillis()).toJobParameters();

        JobExecution jobExecution = jobLauncher.run(indexCrossRefJob, jobParameters);
        BatchStatus status = jobExecution.getStatus();
        assertEquals(status, BatchStatus.COMPLETED);

        Page<CrossRefDocument> response = template.query(SolrCollection.crossref.name(), new SimpleQuery("*:*"),CrossRefDocument.class);
        assertNotNull(response);
        assertTrue(response.getTotalElements() >= 171);

        // clean up
        template.delete(SolrCollection.crossref.name(), new SimpleQuery("*:*"));
        template.commit(SolrCollection.crossref.name());

    }
}