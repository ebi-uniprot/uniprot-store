package org.uniprot.store.indexer.help;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.uniprot.store.indexer.help.HelpPageItemReaderTest.ABOUT_CONTENT;
import static org.uniprot.store.indexer.help.HelpPageItemReaderTest.THREE_D_CONTENT;

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
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.help.HelpDocument;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            ListenerConfig.class,
            HelpPageIndexStep.class,
            HelpPageIndexJob.class
        })
class HelpPagesLoadJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testDiseaseLoadJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.HELP_PAGE_INDEX_JOB_NAME));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        StepExecution indexingStep =
                jobExecution.getStepExecutions().stream()
                        .filter(step -> step.getStepName().equals(Constants.HELP_PAGE_INDEX_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(indexingStep.getReadCount(), is(2));
        assertThat(indexingStep.getWriteCount(), is(2));

        List<HelpDocument> response =
                solrClient.query(SolrCollection.help, new SolrQuery("*:*"), HelpDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(2));
        HelpDocument doc3D = response.get(0);
        assertThat(doc3D.getId(), is("3d-structure_annotation_in_swiss-prot"));
        assertThat(doc3D.getTitle(), is("3D-structure annotation in UniProtKB/Swiss-Prot"));
        assertThat(doc3D.getCategories(), containsInAnyOrder("3D structure", "Biocuration", "Cross-references", "help"));
        assertThat(doc3D.getContent(), is(THREE_D_CONTENT));
        HelpDocument about = response.get(1);
        assertThat(about.getId(), is("about"));
        assertThat(about.getTitle(), is("About UniProt"));
        assertThat(about.getCategories(), containsInAnyOrder("About UniProt", "Staff", "UniProtKB", "UniRef", "UniParc", "help"));
        assertThat(about.getContent(), is(ABOUT_CONTENT));


        // clean up
        solrClient.delete(SolrCollection.help, "*:*");
        solrClient.commit(SolrCollection.help);
    }
}
