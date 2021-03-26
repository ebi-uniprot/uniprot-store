package org.uniprot.store.indexer.publication.uniprotkb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.indexer.publication.PublicationITUtil.createLargeScaleLiterature;
import static org.uniprot.store.indexer.publication.PublicationITUtil.extractObject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
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
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.publication.UniProtKBMappedReference;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.publication.common.LargeScaleStep;
import org.uniprot.store.indexer.publication.common.PublicationJobExecutionListener;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.search.document.publication.PublicationDocument;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            ListenerConfig.class,
            UniProtKBPublicationJob.class,
            UniProtKBPublicationStep.class,
            LargeScaleStep.class,
            PublicationJobExecutionListener.class
        })
class UniProtKBPublicationJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    @BeforeEach
    void setupSolr() throws Exception {
        LiteratureDocument litDoc = createLargeScaleLiterature(29748402);
        solrClient.saveBeans(SolrCollection.literature, Collections.singleton(litDoc));
        solrClient.commit(SolrCollection.literature);
    }

    @Test
    void testUniProtKBPublicationIndexingJob() throws Exception {
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
                new SolrQuery("types:" + MappedReferenceType.UNIPROTKB_UNREVIEWED.getIntValue());
        List<PublicationDocument> docs =
                solrClient.query(
                        SolrCollection.publication, allUniProtPubs, PublicationDocument.class);
        assertThat(docs, hasSize(6));
        for (PublicationDocument doc : docs) {
            assertThat(doc.getId(), is(notNullValue()));
            assertThat(
                    doc.getMainType(), is(MappedReferenceType.UNIPROTKB_UNREVIEWED.getIntValue()));
            assertThat(doc.getRefNumber(), is(notNullValue()));
            MappedPublications mappedPubs = extractObject(doc);
            assertThat(mappedPubs, is(notNullValue()));
            assertThat(mappedPubs.getUniProtKBMappedReference(), is(notNullValue()));
            assertThat(mappedPubs.getComputationallyMappedReferences(), is(empty()));
            assertThat(mappedPubs.getCommunityMappedReferences(), is(empty()));
            UniProtKBMappedReference mappedRef = mappedPubs.getUniProtKBMappedReference();
            assertThat(mappedRef, is(notNullValue()));
            assertThat(mappedRef.getUniProtKBAccession(), is(notNullValue()));
            assertThat(mappedRef.getUniProtKBAccession().getValue(), is(notNullValue()));
            assertThat(mappedRef.getSource(), is(notNullValue()));
            assertThat(mappedRef.getSource().getName(), is(notNullValue()));
            assertThat(mappedRef.getSource().getId(), is(nullValue()));
            assertThat(mappedRef.getSourceCategories(), hasSize(1));
            assertThat(mappedRef.getReferenceComments(), hasSize(1));
            assertThat(mappedRef.getReferencePositions(), hasSize(1));
        }

        // get accession with one publication without pubmed id
        SolrQuery accessionQuery = new SolrQuery("accession:A0A2Z5SLI5");
        List<PublicationDocument> accDocs =
                solrClient.query(
                        SolrCollection.publication, accessionQuery, PublicationDocument.class);
        assertThat(accDocs, hasSize(2));
        assertThat(accDocs.get(0).getCitationId(), is(notNullValue()));
        PublicationDocument accDoc = accDocs.get(0);
        assertThat(accDoc.getCitationId(), is("29748402"));
        assertThat(accDoc.isLargeScale(), is(true));
        MappedPublications mappedPubs = extractObject(accDoc);
        assertThat(mappedPubs.getCommunityMappedReferences(), is(emptyIterable()));
        assertThat(mappedPubs.getComputationallyMappedReferences(), is(emptyIterable()));
        assertThat(mappedPubs.getUniProtKBMappedReference(), is(notNullValue()));
        UniProtKBMappedReference reference = mappedPubs.getUniProtKBMappedReference();
        assertThat(reference.getCitationId(), is("29748402"));
        assertThat(reference.getUniProtKBAccession(), is(notNullValue()));
        assertThat(reference.getUniProtKBAccession().getValue(), is("A0A2Z5SLI5"));
        assertThat(reference.getReferenceComments(), is(not(emptyIterable())));
        assertThat(reference.getReferenceComments().get(0).getValue(), is("YM18"));
        assertThat(reference.getReferencePositions(), is(not(emptyIterable())));
        assertThat(
                reference.getReferencePositions().get(0),
                is("NUCLEOTIDE SEQUENCE [LARGE SCALE GENOMIC DNA]"));

        // without pubmedid
        accDoc = accDocs.get(1);
        assertThat(accDoc.getCitationId(), is("CI-52DGOONA53OCB"));
        assertThat(accDoc.isLargeScale(), is(false));
        mappedPubs = extractObject(accDocs.get(1));
        assertThat(mappedPubs.getCommunityMappedReferences(), is(emptyIterable()));
        assertThat(mappedPubs.getComputationallyMappedReferences(), is(emptyIterable()));
        assertThat(mappedPubs.getUniProtKBMappedReference(), is(notNullValue()));
        reference = mappedPubs.getUniProtKBMappedReference();
        assertThat(reference.getCitationId(), is("CI-52DGOONA53OCB"));
        assertThat(reference.getUniProtKBAccession(), is(notNullValue()));
        assertThat(reference.getUniProtKBAccession().getValue(), is("A0A2Z5SLI5"));
        assertThat(reference.getReferenceComments(), is(not(emptyIterable())));
        assertThat(reference.getReferenceComments().get(0).getValue(), is("Cervix carcinoma"));
        assertThat(reference.getReferencePositions(), is(not(emptyIterable())));
        assertThat(reference.getReferencePositions().get(0), is("NUCLEOTIDE SEQUENCE [MRNA]"));
    }
}
