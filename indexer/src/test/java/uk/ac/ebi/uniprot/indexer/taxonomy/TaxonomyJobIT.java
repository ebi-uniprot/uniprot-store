package uk.ac.ebi.uniprot.indexer.taxonomy;

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
import org.springframework.data.domain.Sort;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.taxonomy.steps.*;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.FakeReadDatabaseConfig;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class, FakeReadDatabaseConfig.class,
        ListenerConfig.class, TaxonomyCountStep.class, TaxonomyNamesStep.class, TaxonomyNodeStep.class,
        TaxonomyStrainStep.class, TaxonomyURLStep.class,TaxonomyVirusHostStep.class,TaxonomyJob.class})
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

        TaxonomyDocument taxonomyDocument = response.getContent().get(0);
        assertThat(taxonomyDocument,is(notNullValue()));
        assertThat(taxonomyDocument.getId(),is("1"));
        assertThat(taxonomyDocument.getTaxId(),is(1L));
        assertThat(taxonomyDocument.getSwissprotCount(),is(2L));
        assertThat(taxonomyDocument.getTremblCount(),is(2L));
        assertThat(taxonomyDocument.getAncestor(),is(5L));
        assertThat(taxonomyDocument.getScientific(),is("Sptr_Scientific_1"));
        assertThat(taxonomyDocument.getCommon(),is("Sptr_Common_1"));
        assertThat(taxonomyDocument.getSynonym(),is("sptr_synonym_1"));
        assertThat(taxonomyDocument.getMnemonic(),is("Tax_Code_1"));
        assertThat(taxonomyDocument.getRank(),is("rank_1"));

        assertThat(taxonomyDocument.getHost(),contains(4L,5L));
        assertThat(taxonomyDocument.getStrain(),contains("strain 2, strain 1"));
        assertThat(taxonomyDocument.getOtherNames(),contains("first name","second name"));
        assertThat(taxonomyDocument.getUrl(),contains("uri 1","uri 2"));

        assertThat(taxonomyDocument.isHidden(),is(true));
    }

}