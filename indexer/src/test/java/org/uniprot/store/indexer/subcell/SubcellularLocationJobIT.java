package org.uniprot.store.indexer.subcell;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.CoreMatchers;
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
import org.uniprot.core.cv.subcell.SubcellLocationCategory;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryImpl;
import org.uniprot.core.json.parser.subcell.SubcellularLocationJsonConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.FakeReadDatabaseConfig;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            FakeReadDatabaseConfig.class,
            ListenerConfig.class,
            SubcellularLocationJob.class,
            SubcellularLocationLoadStep.class,
            SubcellularLocationStatisticsStep.class,
            SubcellularLocationLoadProcessor.class
        })
// to inject job execution...
class SubcellularLocationJobIT {

    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testSubcellularLocationIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.SUBCELLULAR_LOCATION_LOAD_JOB_NAME));

        // Validating job and status execution
        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Map<String, StepExecution> stepMap =
                jobExecution.getStepExecutions().stream()
                        .collect(
                                Collectors.toMap(
                                        StepExecution::getStepName,
                                        stepExecution -> stepExecution));

        assertThat(stepMap, is(notNullValue()));
        assertThat(stepMap.containsKey(Constants.SUBCELLULAR_LOCATION_INDEX_STEP), is(true));
        StepExecution step = stepMap.get(Constants.SUBCELLULAR_LOCATION_INDEX_STEP);
        assertThat(step.getReadCount(), is(520));
        assertThat(step.getWriteCount(), is(520));

        // Validating if solr document was written correctly
        SolrQuery solrQuery = new SolrQuery("*:*").setRows(600);
        solrQuery.addSort(SolrQuery.SortClause.asc("id"));
        List<SubcellularLocationDocument> response =
                solrClient.query(
                        SolrCollection.subcellularlocation,
                        solrQuery,
                        SubcellularLocationDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(520));

        // search by definition
        solrQuery = new SolrQuery("definition:alternating");
        response =
                solrClient.query(
                        SolrCollection.subcellularlocation,
                        solrQuery,
                        SubcellularLocationDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(2));

        // search by synonym
        solrQuery = new SolrQuery("synonym:Flagellar");
        response =
                solrClient.query(
                        SolrCollection.subcellularlocation,
                        solrQuery,
                        SubcellularLocationDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(8));

        // validating if can search one single entry with mapped and cited items
        solrQuery = new SolrQuery("id:SL-0188");
        response =
                solrClient.query(
                        SolrCollection.subcellularlocation,
                        solrQuery,
                        SubcellularLocationDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));

        SubcellularLocationDocument subcellularLocationDocument = response.get(0);
        validateSubcellularLocationDocument(subcellularLocationDocument);

        byte[] byteArray = subcellularLocationDocument.getSubcellularlocationObj();
        ObjectMapper jsonMapper = SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
        SubcellularLocationEntry entry =
                jsonMapper.readValue(byteArray, SubcellularLocationEntryImpl.class);
        validateSubcellularLocationEntry(entry);
    }

    private void validateSubcellularLocationEntry(SubcellularLocationEntry entry) {
        assertThat(entry, is(notNullValue()));
        assertThat(entry.getName(), is("Nucleolus"));
        assertThat(entry.getCategory(), is(SubcellLocationCategory.LOCATION));
        assertThat(entry.getId(), is("SL-0188"));
        assertThat(entry.getContent(), is("Nucleus, nucleolus"));
        assertThat(
                entry.getDefinition(), startsWith("The nucleolus is a non-membrane bound nuclear"));

        assertThat(entry.getGeneOntologies(), is(notNullValue()));
        assertThat(entry.getGeneOntologies().size(), is(1));

        assertThat(entry.getSynonyms(), is(notNullValue()));
        assertThat(entry.getSynonyms().size(), is(1));

        assertThat(entry.getStatistics(), is(notNullValue()));
        assertThat(entry.getStatistics().getReviewedProteinCount(), is(5L));
        assertThat(entry.getStatistics().getUnreviewedProteinCount(), is(6L));
    }

    private void validateSubcellularLocationDocument(
            SubcellularLocationDocument subcellularLocationDocument) {
        assertThat(subcellularLocationDocument, is(notNullValue()));
        assertThat(subcellularLocationDocument.getId(), is(notNullValue()));
        assertThat(subcellularLocationDocument.getId(), is("SL-0188"));
        assertThat(subcellularLocationDocument.getSubcellularlocationObj(), is(notNullValue()));
    }
}
