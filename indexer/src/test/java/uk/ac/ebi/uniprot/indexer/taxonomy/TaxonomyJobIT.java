package uk.ac.ebi.uniprot.indexer.taxonomy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.domain.taxonomy.*;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyEntryBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.impl.TaxonomyEntryImpl;
import uk.ac.ebi.uniprot.domain.uniprot.taxonomy.Taxonomy;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.taxonomy.processor.TaxonomyProcessor;
import uk.ac.ebi.uniprot.indexer.taxonomy.steps.TaxonomyNodeStep;
import uk.ac.ebi.uniprot.indexer.taxonomy.steps.TaxonomyStatisticsStep;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.FakeReadDatabaseConfig;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.json.parser.taxonomy.TaxonomyJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class, FakeReadDatabaseConfig.class,
        ListenerConfig.class, TaxonomyJobIT.TaxonomyNodeStepFake.class, TaxonomyJobIT.TaxonomyStatisticsStepFake.class,
        TaxonomyJob.class})
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

        assertThat(stepMap.containsKey(Constants.TAXONOMY_LOAD_STATISTICS_STEP_NAME),is(true));
        step = stepMap.get(Constants.TAXONOMY_LOAD_STATISTICS_STEP_NAME);
        assertThat(step.getReadCount(), is(2));
        assertThat(step.getWriteCount(), is(2));

        //Validating if solr document was written correctly
        SimpleQuery solrQuery = new SimpleQuery("*:*");
        solrQuery.addSort(new Sort(Sort.Direction.ASC,"id"));
        Page<TaxonomyDocument> response = template
                .query(SolrCollection.taxonomy.name(),solrQuery , TaxonomyDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(5L));

        TaxonomyDocument taxonomyDocument = response.getContent().get(4);
        validateTaxonomyDocument(taxonomyDocument);

        assertThat(taxonomyDocument.getTaxonomyObj(),is(notNullValue()));
        ByteBuffer byteBuffer = taxonomyDocument.getTaxonomyObj();

        ObjectMapper jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
        TaxonomyEntry entry =  jsonMapper.readValue(byteBuffer.array(), TaxonomyEntryImpl.class);
        validateTaxonomyEntry(entry);
    }

    private void validateTaxonomyDocument(TaxonomyDocument taxonomyDocument) {
        assertThat(taxonomyDocument,is(notNullValue()));
        assertThat(taxonomyDocument.getId(),is("5"));
        assertThat(taxonomyDocument.getTaxId(),is(5L));
        assertThat(taxonomyDocument.isActive(),is(true));
        assertThat(taxonomyDocument.getAncestor(),is(4L));
        assertThat(taxonomyDocument.getScientific(),is("Sptr_Scientific_5"));
        assertThat(taxonomyDocument.getCommon(),is("Sptr_Common_5"));
        assertThat(taxonomyDocument.getSynonym(),is("sptr_synonym_5"));
        assertThat(taxonomyDocument.getMnemonic(),is("Tax_Code_5"));
        assertThat(taxonomyDocument.getRank(),is("family"));

        assertThat(taxonomyDocument.getHost(),contains(4L,5L));
        assertThat(taxonomyDocument.getStrain(),contains("strain 1 ; strain 1,syn 1  , strain 1,syn 2", "strain 2 ; strain 2 syn 1"));
        assertThat(taxonomyDocument.isLinked(),is(true));
        assertThat(taxonomyDocument.getLineage(),contains(4L));
        assertThat(taxonomyDocument.isHidden(),is(true));
    }

    private void validateTaxonomyEntry(TaxonomyEntry entry) {
        assertThat(entry, is(notNullValue()));

        assertThat(entry.getTaxonId(),is(5L));
        assertThat(entry.isActive(),is(true));
        assertThat(entry.getParentId(),is(4L));
        assertThat(entry.getScientificName(),is("Sptr_Scientific_5"));
        assertThat(entry.getCommonName(),is("Sptr_Common_5"));
        assertThat(entry.getSynonyms(),contains("sptr_synonym_5"));
        assertThat(entry.getMnemonic(),is("Tax_Code_5"));
        assertThat(entry.getRank(),is(TaxonomyRank.FAMILY));
        assertThat(entry.isHidden(),is(true));

        assertThat(entry.getHosts(), is(notNullValue()));
        assertThat(entry.getHosts().size(), is(2));
        Taxonomy host = entry.getHosts().get(0);
        assertThat(host.getTaxonId(),is(4L));
        assertThat(host.getScientificName(),is("Sptr_Scientific_4"));
        assertThat(host.getCommonName(),is("Sptr_Common_4"));
        assertThat(host.getMnemonic(),is("Tax_Code_4"));
        assertThat(host.getSynonyms(),contains("sptr_synonym_4"));

        assertThat(entry.getStrains(), is(notNullValue()));
        assertThat(entry.getStrains().size(), is(2));
        TaxonomyStrain strain = entry.getStrains().get(0);
        assertThat(strain.getName(),is("strain 1"));
        assertThat(strain.getSynonyms(),contains("strain 1,syn 1 ","strain 1,syn 2"));


        assertThat(entry.getLineage(), is(notNullValue()));
        assertThat(entry.getLineage().size(), is(1));
        TaxonomyLineage lineage = entry.getLineage().get(0);
        assertThat(lineage.getTaxonId(),is(4L));
        assertThat(lineage.getScientificName(),is("name4"));
        assertThat(lineage.getRank(),is(TaxonomyRank.KINGDOM));
        assertThat(lineage.isHidden(),is(true));

        assertThat(entry.getStatistics(), is(notNullValue()));
        TaxonomyStatistics statistics = entry.getStatistics();
        assertThat(statistics.getReviewedProteinCount(),is(6L));
        assertThat(statistics.getUnreviewedProteinCount(),is(2L));
        assertThat(statistics.getProteomeCount(),is(1L));

    }

    @Configuration
    static class TaxonomyNodeStepFake extends TaxonomyNodeStep{

        @Override
        @Bean(name = "itemTaxonomyNodeProcessor")
        public ItemProcessor<TaxonomyEntryBuilder,TaxonomyDocument> itemTaxonomyNodeProcessor(@Qualifier("readDataSource") DataSource readDataSource){
            return new TaxonomyProcessorFake(readDataSource);
        }
    }

    @Configuration
    static class TaxonomyStatisticsStepFake extends TaxonomyStatisticsStep{

        @Override
        protected String getStatisticsSQL(){
            return TaxonomySQLConstants.COUNT_PROTEINS_SQL.replaceAll("full join","inner join");
        }

    }

    private static class TaxonomyProcessorFake extends TaxonomyProcessor {

        public TaxonomyProcessorFake(DataSource readDataSource){
            super(readDataSource);
        }

        @Override
        protected String getTaxonomyLineageSQL(){
            return "select '|5|4|1' as lineage_id," +
                    "      '|name5|name4|name1' AS lineage_name," +
                    "      '|rank5|KINGDOM|rank1' AS lineage_rank," +
                    "      '|0|1|0' AS lineage_hidden" +
                    " from taxonomy.V_PUBLIC_NODE" +
                    " WHERE TAX_ID = ?";
        }
    }

}