package uk.ac.ebi.uniprot.indexer.crossref;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.crossref.steps.CrossRefStep;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class, CrossRefJob.class, CrossRefStep.class, ListenerConfig.class})
class CrossRefJobTest {

/*    @Autowired
    private JobLauncherTestUtils jobLauncher;
    @Autowired
    private SolrTemplate template;

    @Test
    void testIndexerJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        BatchStatus status = jobExecution.getStatus();
        assertEquals(status, BatchStatus.COMPLETED);

        Page<CrossRefDocument> response = template.query(SolrCollection.crossref.name(), new SimpleQuery("*:*"),CrossRefDocument.class);
        assertNotNull(response);
        assertTrue(response.getTotalElements() >= 171);

        // clean up
        template.delete(SolrCollection.crossref.name(), new SimpleQuery("*:*"));
        template.commit(SolrCollection.crossref.name());

    }*/
}