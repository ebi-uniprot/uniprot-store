package uk.ac.ebi.uniprot.indexer.keyword;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.solr.core.SolrOperations;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.cv.keyword.KeywordEntry;
import uk.ac.ebi.uniprot.cv.keyword.impl.KeywordEntryImpl;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.FakeReadDatabaseConfig;
import uk.ac.ebi.uniprot.indexer.test.config.SolrTestConfig;
import uk.ac.ebi.uniprot.json.parser.keyword.KeywordJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.keyword.KeywordDocument;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author lgonzales
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, SolrTestConfig.class, FakeReadDatabaseConfig.class,
        ListenerConfig.class, KeywordJob.class, KeywordLoadStep.class, KeywordJobIT.KeywordStatisticsStepFake.class})
class KeywordJobIT {

    @Autowired
    private JobLauncherTestUtils jobLauncher;

    @Autowired
    private SolrOperations solrOperations;

    @Test
    void testKeywordIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(Constants.KEYWORD_LOAD_JOB_NAME));


        //Validating job and status execution
        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Map<String, StepExecution> stepMap = jobExecution.getStepExecutions().stream()
                .collect(Collectors.toMap(StepExecution::getStepName, stepExecution -> stepExecution));

        assertThat(stepMap, is(notNullValue()));
        assertThat(stepMap.containsKey(Constants.KEYWORD_INDEX_STEP), is(true));
        StepExecution step = stepMap.get(Constants.KEYWORD_INDEX_STEP);
        assertThat(step.getReadCount(), is(1199));
        assertThat(step.getWriteCount(), is(1199));

        //Validating if solr document was written correctly
        SimpleQuery solrQuery = new SimpleQuery("*:*");
        solrQuery.addSort(new Sort(Sort.Direction.ASC, "id"));
        Page<KeywordDocument> response = solrOperations
                .query(SolrCollection.keyword.name(), solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(1199L));


        //validating if can search one single entry
        solrQuery = new SimpleQuery("id:KW-0540");
        response = solrOperations
                .query(SolrCollection.keyword.name(), solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(1L));

        KeywordDocument keywordDocument = response.getContent().get(0);
        validateKeywordDocument(keywordDocument);

        ByteBuffer byteBuffer = keywordDocument.getKeywordObj();
        ObjectMapper jsonMapper = KeywordJsonConfig.getInstance().getFullObjectMapper();
        KeywordEntry entry = jsonMapper.readValue(byteBuffer.array(), KeywordEntryImpl.class);
        validateKeywordDetail(entry);

        //validating if can search one category entry
        solrQuery = new SimpleQuery("id:KW-9993");
        response = solrOperations
                .query(SolrCollection.keyword.name(), solrQuery, KeywordDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(1L));

        keywordDocument = response.getContent().get(0);
        byteBuffer = keywordDocument.getKeywordObj();
        entry = jsonMapper.readValue(byteBuffer.array(), KeywordEntryImpl.class);
        assertThat(entry, is(notNullValue()));
        assertThat(entry.getStatistics(), is(notNullValue()));
        assertThat(entry.getStatistics().getReviewedProteinCount(), is(3L));
        assertThat(entry.getStatistics().getUnreviewedProteinCount(), is(1L));
    }

    private void validateKeywordDetail(KeywordEntry entry) {
        assertThat(entry.getKeyword(), is(notNullValue()));
        assertThat(entry.getKeyword().getAccession(), is(notNullValue()));
        assertThat(entry.getKeyword().getAccession(), is("KW-0540"));
        assertThat(entry.getKeyword().getId(), is(notNullValue()));
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

    @Configuration
    static class KeywordStatisticsStepFake extends KeywordStatisticsStep {

        @Override
        protected String getStatisticsSQL() {
            return KeywordSQLConstants.KEYWORD_STATISTICS_URL.replaceAll("FULL JOIN", "INNER JOIN");
        }

    }
}