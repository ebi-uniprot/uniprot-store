package uk.ac.ebi.uniprot.indexer.taxonomy;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyLineageReader;
import uk.ac.ebi.uniprot.indexer.taxonomy.steps.*;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.FakeReadDatabaseConfig;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class, FakeReadDatabaseConfig.class,
        ListenerConfig.class, TaxonomyCountStep.class, TaxonomyNamesStep.class, TaxonomyNodeStep.class,
        TaxonomyStrainStep.class, TaxonomyURLStep.class,TaxonomyVirusHostStep.class,TaxonomyJob.class,
        TaxonomyJobIT.TaxonomyLineageStepFake.class})
class TaxonomyJobIT {

    @Autowired
    private JobLauncherTestUtils jobLauncher;

    @Autowired
    private SolrTemplate template;

    @Test
    void testTaxonomyIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(Constants.TAXONOMY_LOAD_JOB_NAME));


        //Validating job and status execution
        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        Map<String,StepExecution> stepMap = jobExecution.getStepExecutions().stream()
                .collect(Collectors.toMap(StepExecution::getStepName,stepExecution -> stepExecution));

        assertThat(stepMap,is(notNullValue()));
        assertThat(stepMap.containsKey(Constants.TAXONOMY_LOAD_NODE_STEP_NAME),is(true));
        StepExecution step = stepMap.get(Constants.TAXONOMY_LOAD_NODE_STEP_NAME);
        assertThat(step.getReadCount(), is(5));
        assertThat(step.getWriteCount(), is(5));

        assertThat(stepMap.containsKey(Constants.TAXONOMY_LOAD_URL_STEP_NAME),is(true));
        step = stepMap.get(Constants.TAXONOMY_LOAD_URL_STEP_NAME);
        assertThat(step.getReadCount(), is(4));
        assertThat(step.getWriteCount(), is(4));

        assertThat(stepMap.containsKey(Constants.TAXONOMY_LOAD_COUNT_STEP_NAME),is(true));
        step = stepMap.get(Constants.TAXONOMY_LOAD_COUNT_STEP_NAME);
        assertThat(step.getReadCount(), is(2));
        assertThat(step.getWriteCount(), is(2));

        assertThat(stepMap.containsKey(Constants.TAXONOMY_LOAD_NAMES_STEP_NAME),is(true));
        step = stepMap.get(Constants.TAXONOMY_LOAD_NAMES_STEP_NAME);
        assertThat(step.getReadCount(), is(2));
        assertThat(step.getWriteCount(), is(2));

        assertThat(stepMap.containsKey(Constants.TAXONOMY_LOAD_STRAIN_STEP_NAME),is(true));
        step = stepMap.get(Constants.TAXONOMY_LOAD_STRAIN_STEP_NAME);
        assertThat(step.getReadCount(), is(2));
        assertThat(step.getWriteCount(), is(2));

        assertThat(stepMap.containsKey(Constants.TAXONOMY_LOAD_VIRUS_HOST_STEP_NAME),is(true));
        step = stepMap.get(Constants.TAXONOMY_LOAD_VIRUS_HOST_STEP_NAME);
        assertThat(step.getReadCount(), is(4));
        assertThat(step.getWriteCount(), is(4));


        //Validating if solr document was written correctly
        SimpleQuery solrQuery = new SimpleQuery("*:*");
        solrQuery.addSort(new Sort(Sort.Direction.ASC,"id"));
        Page<TaxonomyDocument> response = template
                .query(SolrCollection.taxonomy.name(),solrQuery , TaxonomyDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(5L));

        TaxonomyDocument taxonomyDocument = response.getContent().get(4);
        assertThat(taxonomyDocument,is(notNullValue()));
        assertThat(taxonomyDocument.getId(),is("5"));
        assertThat(taxonomyDocument.getTaxId(),is(5L));
        assertThat(taxonomyDocument.isActive(),is(true));
        assertThat(taxonomyDocument.getSwissprotCount(),is(2L));
        assertThat(taxonomyDocument.getTremblCount(),is(2L));
        assertThat(taxonomyDocument.getAncestor(),is(4L));
        assertThat(taxonomyDocument.getScientific(),is("Sptr_Scientific_5"));
        assertThat(taxonomyDocument.getCommon(),is("Sptr_Common_5"));
        assertThat(taxonomyDocument.getSynonym(),is("sptr_synonym_5"));
        assertThat(taxonomyDocument.getMnemonic(),is("Tax_Code_5"));
        assertThat(taxonomyDocument.getRank(),is("rank_5"));

        assertThat(taxonomyDocument.getHost(),contains(4L,5L));
        assertThat(taxonomyDocument.getStrain(),contains("strain 4, strain 3"));
        assertThat(taxonomyDocument.getOtherNames(),contains("first name","second name"));
        assertThat(taxonomyDocument.getUrl(),contains("uri 1","uri 2"));
        assertThat(taxonomyDocument.isLinked(),is(true));
        assertThat(taxonomyDocument.getLineage(),contains(4L));

        assertThat(taxonomyDocument.isHidden(),is(true));
    }

    @Configuration
    static class TaxonomyLineageStepFake extends TaxonomyLineageStep{

        public TaxonomyLineageStepFake(){

        }

        @Bean(name = "itemTaxonomyLineageReader")
        public ItemReader<TaxonomyDocument> itemTaxonomyLineageReader(DataSource readDataSource) throws SQLException {
            JdbcCursorItemReader<TaxonomyDocument> itemReader = new JdbcCursorItemReader<>();
            itemReader.setDataSource(readDataSource);
            itemReader.setSql("select '|5|4|1' as lineage_id," +
                    "      '|name5|name4|name1' AS lineage_name," +
                    "      '|rank5|rank4|rank1' AS lineage_rank," +
                    "      '|0|1|0' AS lineage_hidden" +
                    " from taxonomy.V_PUBLIC_NODE" +
                    " WHERE TAX_ID = 1");
            itemReader.setRowMapper(new TaxonomyLineageReader());

            return itemReader;
        }

    }

}