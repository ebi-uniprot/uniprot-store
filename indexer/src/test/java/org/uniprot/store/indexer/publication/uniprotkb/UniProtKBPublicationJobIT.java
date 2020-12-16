package org.uniprot.store.indexer.publication.uniprotkb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
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
import org.uniprot.core.json.parser.publication.UniProtKBMappedReferenceJsonConfig;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.publication.UniProtKBMappedReference;
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
            UniProtKBPublicationJob.class,
            UniProtKBPublicationStep.class
        })
class UniProtKBPublicationJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    private final ObjectMapper objectMapper =
            UniProtKBMappedReferenceJsonConfig.getInstance().getFullObjectMapper();

    @Test
    void testCommunityPublicationIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.UNIPROTKB_PUBLICATION_JOB_NAME));

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
        assertThat(stepMap.containsKey(Constants.UNIPROTKB_PUBLICATION_INDEX_STEP), is(true));
        StepExecution step = stepMap.get(Constants.UNIPROTKB_PUBLICATION_INDEX_STEP);
        assertThat(step.getReadCount(), is(5));
        assertThat(step.getWriteCount(), is(5));
        // ---------- check "type" field
        SolrQuery allUniProtPubs =
                new SolrQuery("type:" + MappedReferenceType.UNIPROTKB_UNREVIEWED.getIntValue());
        List<PublicationDocument> docs =
                solrClient.query(
                        SolrCollection.publication, allUniProtPubs, PublicationDocument.class);
        assertThat(docs, hasSize(5));
        for (PublicationDocument doc : docs) {
            UniProtKBMappedReference mappedRef = extractObject(doc);
            assertThat(mappedRef, is(notNullValue()));
            assertThat(mappedRef.getUniProtKBAccession(), is(notNullValue()));
            assertThat(mappedRef.getUniProtKBAccession().getValue(), is(notNullValue()));
            assertThat(mappedRef.getPubMedId(), is(notNullValue()));
            assertThat(mappedRef.getSource(), is(notNullValue()));
            assertThat(mappedRef.getSource().getName(), is(notNullValue()));
            assertThat(mappedRef.getSource().getId(), is(nullValue()));
            assertThat(mappedRef.getSourceCategories(), hasSize(1));
            assertThat(mappedRef.getReferenceComments(), hasSize(1));
            assertThat(mappedRef.getReferencePositions(), hasSize(1));
        }
    }

    private UniProtKBMappedReference extractObject(PublicationDocument document)
            throws IOException {
        return objectMapper.readValue(
                document.getPublicationMappedReference(), UniProtKBMappedReference.class);
    }
}
