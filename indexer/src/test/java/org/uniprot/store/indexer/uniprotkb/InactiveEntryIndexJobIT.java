package org.uniprot.store.indexer.uniprotkb;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.uniprot.store.indexer.common.utils.Constants.INACTIVEENTRY_INDEX_JOB;
import static org.uniprot.store.indexer.common.utils.Constants.INACTIVEENTRY_INDEX_STEP;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import org.uniprot.store.indexer.uniprotkb.step.InactiveEntryStep;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.job.common.util.CommonConstants;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 *
 * @author jluo
 * @date: 5 Sep 2019
 *
*/
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {SolrTestConfig.class, FakeIndexerSpringBootApplication.class, InactiveEntryIndexJob.class,
                           InactiveEntryStep.class, ListenerConfig.class})
@TestPropertySource(properties = {"uniprotkb.indexing.itemProcessorTaskExecutor.corePoolSize=1",
        "uniprotkb.indexing.itemProcessorTaskExecutor.maxPoolSize=1",
        "uniprotkb.indexing.itemWriterTaskExecutor.corePoolSize=1",
        "uniprotkb.indexing.itemWriterTaskExecutor.maxPoolSize=1"})
public class InactiveEntryIndexJobIT {
	@Autowired
    private JobLauncherTestUtils jobLauncher;
    @Autowired
    private UniProtSolrOperations solrOperations;
    @Autowired
    private UniProtKBIndexingProperties indexingProperties;

    @Test
    void testUniProtKBIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(INACTIVEENTRY_INDEX_JOB));

       Thread.sleep(5000);

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        assertThat(stepExecutions, hasSize(1));

        checkInactiveEntryIndexingStep(jobExecution, stepExecutions);
     
    }

    private void checkInactiveEntryIndexingStep(JobExecution jobExecution, Collection<StepExecution> stepExecutions)
            throws IOException {
        StepExecution kbIndexingStep = stepExecutions.stream()
                .filter(step -> step.getStepName().equals(INACTIVEENTRY_INDEX_STEP))
                .collect(Collectors.toList()).get(0);

        assertThat(kbIndexingStep.getReadCount(), is(22));
        checkWriteCount(jobExecution, CommonConstants.FAILED_ENTRIES_COUNT_KEY, 0);
        checkWriteCount(jobExecution, CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, 22);

        // check that the accessions in the source file, are the ones that were written to Solr
        Set<String> sourceAccessions = readSourceAccessions();
        assertThat(sourceAccessions, hasSize(22));

        SimpleQuery query = new SimpleQuery("*:*");
   
        query.setPageRequest( PageRequest.of(0, 30));
        Page<UniProtDocument> response = solrOperations
                .query(SolrCollection.uniprot.name(), query, UniProtDocument.class);
        
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(22L));
        Set<String> results = response.stream().map(doc -> doc.accession).collect(Collectors.toSet());
        results.forEach(accession -> assertThat(sourceAccessions, hasItem(accession)) );
       
        assertThat(response.stream().map(doc -> doc.accession).collect(Collectors.toSet()),
                   is(sourceAccessions));
    }


    private Set<String> readSourceAccessions() throws IOException {
        return Files.lines(Paths.get(indexingProperties.getInactiveEntryFile()))
                .map(line -> line.substring(0, line.indexOf(",")))
                .collect(Collectors.toSet());
    }

    private void checkWriteCount(JobExecution jobExecution, String uniprotkbIndexFailedEntriesCountKey, int i) {
        AtomicInteger failedCountAI = (AtomicInteger) jobExecution.getExecutionContext()
                .get(uniprotkbIndexFailedEntriesCountKey);
        assertThat(failedCountAI, CoreMatchers.is(notNullValue()));
        assertThat(failedCountAI.get(), CoreMatchers.is(i));
    }
}

