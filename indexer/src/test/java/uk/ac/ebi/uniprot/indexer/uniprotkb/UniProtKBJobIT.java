package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.listeners.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.document.SolrCollection;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;

import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static uk.ac.ebi.uniprot.indexer.common.utils.Constants.UNIPROTKB_INDEX_STEP;

/**
 * Created 11/04/19
 *
 * @author Edd
 */
// TODO: 12/04/19 Incorporate UniRef Map data when Guoying is back
// TODO: 12/04/19 Check failed entry is written to error file
//                  Need to think about this: there can be many paths that lead to error
//                  1. bad FF format -- does the FF iterator write failed entries to an error file?
//                     Unlikely, since the file will be a RELEASE file where lots of checks were done
//                  2. entry->document conversion
//                  3. writing to solr, e.g., due to network issue
//                     Could catch Error class after writing to solr,
//                     Done. Needs testing.
// TODO: 12/04/19 Check failures in presence of fault tolerant step will continue up to certain limit
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class, UniProtKBJob.class,
                           UniProtKBStep.class, ListenerConfig.class})
class UniProtKBJobIT {
    @Autowired
    private JobLauncherTestUtils jobLauncher;
    @Autowired
    private SolrTemplate template;

    @Test
    void testUniProtKBIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Page<UniProtDocument> response = template
                .query(SolrCollection.uniprot.name(), new SimpleQuery("*:*"), UniProtDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(2L));

        StepExecution indexingStep = jobExecution.getStepExecutions().stream()
                .filter(step -> step.getStepName().equals(UNIPROTKB_INDEX_STEP))
                .collect(Collectors.toList()).get(0);

        assertThat(indexingStep.getReadCount(), is(2));
        assertThat(indexingStep.getReadSkipCount(), is(0));
        assertThat(indexingStep.getProcessSkipCount(), is(0));
        assertThat(indexingStep.getWriteSkipCount(), is(0));
        assertThat(indexingStep.getWriteCount(), is(2));
    }
}