package uk.ac.ebi.uniprot.indexer.crossref;

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
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.common.listener.ListenerConfig;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.crossref.steps.CrossRefStep;
import uk.ac.ebi.uniprot.indexer.crossref.steps.CrossRefUniProtCountStep;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.FakeReadDatabaseConfig;
import uk.ac.ebi.uniprot.indexer.test.config.SolrTestConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, SolrTestConfig.class, FakeReadDatabaseConfig.class,
                           ListenerConfig.class, CrossRefStep.class, CrossRefUniProtCountStep.class, CrossRefJob.class})
public class CrossRefJobIT {

    @Autowired
    private JobLauncherTestUtils jobLauncher;

    @Autowired
    private SolrTemplate template;

    @Test
    void testDiseaseLoadJob() throws Exception {
        JobExecution jobExecution = this.jobLauncher.launchJob();
        assertThat(jobExecution.getJobInstance().getJobName(), CoreMatchers.is(Constants.CROSS_REF_LOAD_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        StepExecution indexingStep = jobExecution.getStepExecutions().stream()
                .filter(step -> step.getStepName().equals(Constants.CROSS_REF_INDEX_STEP))
                .collect(Collectors.toList()).get(0);

        assertThat(indexingStep.getReadCount(), is(5));
        assertThat(indexingStep.getWriteCount(), is(5));

        StepExecution countStep = jobExecution.getStepExecutions().stream()
                .filter(step -> step.getStepName().equals(Constants.CROSS_REF_UNIPROT_COUNT_STEP_NAME))
                .collect(Collectors.toList()).get(0);

        assertThat(countStep.getReadCount(), is(5));
        assertThat(countStep.getWriteCount(), is(5));

        // get all the index docs
        Page<CrossRefDocument> response = this.template.query(SolrCollection.crossref.name(),
                new SimpleQuery("*:*"), CrossRefDocument.class);

        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(5L));

        // get one document
        CrossRefDocument xrefDoc = response.get().filter(xref -> "DB-0160".equals(xref.getAccession())).findFirst().get();
        assertThat(xrefDoc.getAccession(), is("DB-0160"));
        assertThat(xrefDoc.getAbbrev(), is("Allergome"));
        assertThat(xrefDoc.getName(), is("Allergome; a platform for allergen knowledge"));
        assertThat(xrefDoc.getPubMedId(), is("19671381"));
        assertThat(xrefDoc.getDoiId(), is("10.1007/s11882-009-0055-9"));
        assertThat(xrefDoc.getLinkType(), is("Explicit"));
        assertThat(xrefDoc.getServer(), is("http://www.allergome.org/"));
        assertThat(xrefDoc.getDbUrl(), is("http://www.allergome.org/script/dettaglio.php?id_molecule=%s"));
        //assertThat(xrefDoc.getCategory(), is("Protein family/group databases")); TODO FIXME
        assertThat(xrefDoc.getReviewedProteinCount(), is(8L));
        assertThat(xrefDoc.getUnreviewedProteinCount(), is(2L));

        // clean up
        this.template.delete(SolrCollection.disease.name(), new SimpleQuery("*:*"));
        this.template.commit(SolrCollection.disease.name());
    }
}

