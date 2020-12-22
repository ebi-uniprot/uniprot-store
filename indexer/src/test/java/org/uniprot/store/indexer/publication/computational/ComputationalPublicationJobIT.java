package org.uniprot.store.indexer.publication.computational;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.core.publication.MappedReferenceType.COMPUTATIONAL;
import static org.uniprot.store.indexer.publication.community.CommunityPublicationJobIT.extractObject;

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
import org.uniprot.core.json.parser.publication.ComputationallyMappedReferenceJsonConfig;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            ListenerConfig.class,
            ComputationalPublicationJob.class,
            ComputationalPublicationStep.class
        })
class ComputationalPublicationJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    private final ObjectMapper objectMapper =
            ComputationallyMappedReferenceJsonConfig.getInstance().getFullObjectMapper();

    @Test
    void testComputationalPublicationIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.COMPUTATIONAL_PUBLICATION_JOB_NAME));

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
        assertThat(stepMap.containsKey(Constants.COMPUTATIONAL_PUBLICATION_INDEX_STEP), is(true));
        StepExecution step = stepMap.get(Constants.COMPUTATIONAL_PUBLICATION_INDEX_STEP);
        assertThat(step.getReadCount(), is(45));
        assertThat(step.getWriteCount(), is(45));

        // ---------- check "type" field
        SolrQuery allCommunityPubs = new SolrQuery("types:" + COMPUTATIONAL.getIntValue());
        allCommunityPubs.set("rows", 50);
        assertThat(
                solrClient.query(
                        SolrCollection.publication, allCommunityPubs, PublicationDocument.class),
                hasSize(45));

        // ---------- check "id" field
        List<PublicationDocument> documents =
                solrClient.query(
                        SolrCollection.publication,
                        new SolrQuery("id:Q7Z583__26551672__4544"),
                        PublicationDocument.class);

        assertThat(documents, hasSize(1));

        PublicationDocument document = documents.get(0);

        MappedPublications publications = extractObject(document);
        List<ComputationallyMappedReference> references =
                publications.getComputationalMappedReferences();
        assertThat(references, hasSize(1));

        ComputationallyMappedReference reference = references.get(0);

        // ----------- check contents of stored object
        assertThat(reference.getPubMedId(), is("26551672"));
        assertThat(reference.getUniProtKBAccession().getValue(), is("Q7Z583"));
        assertThat(reference.getSource().getName(), is("GeneRif"));
        assertThat(reference.getSource().getId(), is("4544"));
        assertThat(
                reference.getAnnotation(),
                is(
                        "Increases in islet MTNR1B expression is associated with type 2 diabetes susceptibility."));
        assertThat(
                reference.getSourceCategories(),
                containsInAnyOrder("Sequences", "Pathology & Biotech"));
    }
}
