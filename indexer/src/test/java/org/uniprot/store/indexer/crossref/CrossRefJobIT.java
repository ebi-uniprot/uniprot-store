package org.uniprot.store.indexer.crossref;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
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
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.crossref.steps.CrossRefStep;
import org.uniprot.store.indexer.crossref.steps.CrossRefUniProtCountStep;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.FakeReadDatabaseConfig;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.dbxref.CrossRefDocument;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            FakeReadDatabaseConfig.class,
            ListenerConfig.class,
            CrossRefStep.class,
            CrossRefUniProtCountStep.class,
            CrossRefJob.class
        })
class CrossRefJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testDiseaseLoadJob() throws Exception {
        JobExecution jobExecution = this.jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.CROSS_REF_LOAD_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        StepExecution indexingStep =
                jobExecution.getStepExecutions().stream()
                        .filter(step -> step.getStepName().equals(Constants.CROSS_REF_INDEX_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(indexingStep.getReadCount(), is(5));
        assertThat(indexingStep.getWriteCount(), is(5));

        StepExecution countStep =
                jobExecution.getStepExecutions().stream()
                        .filter(
                                step ->
                                        step.getStepName()
                                                .equals(
                                                        Constants
                                                                .CROSS_REF_UNIPROT_COUNT_STEP_NAME))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(countStep.getReadCount(), is(5));
        assertThat(countStep.getWriteCount(), is(5));

        // get all the index docs
        List<CrossRefDocument> response =
                this.solrClient.query(
                        SolrCollection.crossref, new SolrQuery("*:*"), CrossRefDocument.class);

        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(5));

        // get one document
        CrossRefDocument xrefDoc =
                response.stream().filter(xref -> "DB-0160".equals(xref.getId())).findFirst().get();
        assertThat(xrefDoc.getId(), is("DB-0160"));
        assertThat(xrefDoc.getAbbrev(), is("Allergome"));
        assertThat(xrefDoc.getName(), is("Allergome; a platform for allergen knowledge"));
        assertThat(xrefDoc.getPubMedId(), is("19671381"));
        assertThat(xrefDoc.getDoiId(), is("10.1007/s11882-009-0055-9"));
        assertThat(xrefDoc.getLinkType(), is("Explicit"));
        assertThat(xrefDoc.getServers(), notNullValue());
        assertThat(xrefDoc.getServers().size(), is(3));
        assertThat(xrefDoc.getServers().get(0), is("http://www.allergome.org/"));
        assertThat(xrefDoc.getServers().get(1), is("http://www.allergomeplus.org/"));
        assertThat(xrefDoc.getServers().get(2), is("http://www.allergomeplusplus.org/"));
        assertThat(
                xrefDoc.getDbUrl(),
                is("http://www.allergome.org/script/dettaglio.php?id_molecule=%s"));
        assertThat(xrefDoc.getCategory(), is("Protein family/group databases"));
        assertThat(xrefDoc.getReviewedProteinCount(), is(1283L));
        assertThat(xrefDoc.getUnreviewedProteinCount(), is(3167L));

        // clean up
        this.solrClient.delete(SolrCollection.disease, "*:*");
        this.solrClient.commit(SolrCollection.disease);
    }
}
