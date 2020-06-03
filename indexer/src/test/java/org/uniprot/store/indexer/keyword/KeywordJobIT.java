package org.uniprot.store.indexer.keyword;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryImpl;
import org.uniprot.core.json.parser.keyword.KeywordJsonConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.FakeReadDatabaseConfig;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.keyword.KeywordDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            FakeReadDatabaseConfig.class,
            ListenerConfig.class,
            KeywordJob.class,
            KeywordLoadStep.class,
            KeywordStatisticsStep.class
        })
class KeywordJobIT {

    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testKeywordIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.KEYWORD_LOAD_JOB_NAME));

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
        assertThat(stepMap.containsKey(Constants.KEYWORD_INDEX_STEP), is(true));
        StepExecution step = stepMap.get(Constants.KEYWORD_INDEX_STEP);
        assertThat(step.getReadCount(), is(1199));
        assertThat(step.getWriteCount(), is(1199));

        // Validating if solr document was written correctly
        SolrQuery solrQuery = new SolrQuery("*:*").setRows(2000);
        solrQuery.addSort(SolrQuery.SortClause.asc("id"));
        List<KeywordDocument> response =
                solrClient.query(SolrCollection.keyword, solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1199));

        // validating if can search one single entry
        solrQuery = new SolrQuery("name:2Fe-2S");
        response = solrClient.query(SolrCollection.keyword, solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));

        // validating if can search one single entry
        solrQuery = new SolrQuery("content:2Fe-2S");
        response = solrClient.query(SolrCollection.keyword, solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(2));

        // validating if can search one single entry
        solrQuery = new SolrQuery("id:KW-0540");
        response = solrClient.query(SolrCollection.keyword, solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));

        KeywordDocument keywordDocument = response.get(0);
        validateKeywordDocument(keywordDocument);

        ByteBuffer byteBuffer = keywordDocument.getKeywordObj();
        ObjectMapper jsonMapper = KeywordJsonConfig.getInstance().getFullObjectMapper();
        KeywordEntry entry = jsonMapper.readValue(byteBuffer.array(), KeywordEntryImpl.class);
        validateKeywordDetail(entry);

        // validating if can search one category entry
        solrQuery = new SolrQuery("id:KW-9993");
        response = solrClient.query(SolrCollection.keyword, solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));

        keywordDocument = response.get(0);
        byteBuffer = keywordDocument.getKeywordObj();
        entry = jsonMapper.readValue(byteBuffer.array(), KeywordEntryImpl.class);
        assertThat(entry, is(notNullValue()));
        assertThat(entry.getStatistics(), is(notNullValue()));
        assertThat(entry.getStatistics().getReviewedProteinCount(), is(3L));
        assertThat(entry.getStatistics().getUnreviewedProteinCount(), is(1L));
    }

    private void validateKeywordDetail(KeywordEntry entry) {
        assertThat(entry.getKeyword(), is(notNullValue()));
        assertThat(entry.getKeyword().getId(), is(notNullValue()));
        assertThat(entry.getKeyword().getId(), is("KW-0540"));
        assertThat(entry.getKeyword().getName(), is(notNullValue()));
        assertThat(entry.getDefinition(), is(notNullValue()));
        assertThat(entry.getCategory(), is(notNullValue()));
        assertThat(entry.getGeneOntologies(), is(notNullValue()));

        assertThat(entry.getParents(), is(notNullValue()));
        assertThat(entry.getParents().isEmpty(), is(false));
        assertThat(entry.getChildren(), is(notNullValue()));
        assertThat(entry.getChildren().isEmpty(), is(false));
    }

    private void validateKeywordDocument(KeywordDocument keywordDocument) {
        assertThat(keywordDocument.getId(), is(notNullValue()));
        assertThat(keywordDocument.getId(), is("KW-0540"));
        assertThat(keywordDocument.getKeywordObj(), is(notNullValue()));
    }
}
