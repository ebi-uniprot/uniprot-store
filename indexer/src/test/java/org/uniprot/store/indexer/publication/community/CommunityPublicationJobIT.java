package org.uniprot.store.indexer.publication.community;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.indexer.publication.PublicationITUtil.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
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
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.publication.PublicationITUtil;
import org.uniprot.store.indexer.publication.common.LargeScaleStep;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            ListenerConfig.class,
            CommunityPublicationJob.class,
            CommunityPublicationStep.class,
            LargeScaleStep.class
        })
class CommunityPublicationJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    @BeforeEach
    void setupSolr() throws Exception{
        LiteratureDocument litDoc = createLargeScaleLiterature(27190215);
        solrClient.saveBeans(SolrCollection.literature, Collections.singleton(litDoc));
        solrClient.commit(SolrCollection.literature);
    }

    @Test
    void testCommunityPublicationIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.COMMUNITY_PUBLICATION_JOB_NAME));

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
        assertThat(stepMap.containsKey(Constants.COMMUNITY_PUBLICATION_INDEX_STEP), is(true));
        StepExecution step = stepMap.get(Constants.COMMUNITY_PUBLICATION_INDEX_STEP);
        assertThat(step.getReadCount(), is(41));
        assertThat(step.getWriteCount(), is(41));

        // ---------- check "type" field
        SolrQuery allCommunityPubs =
                new SolrQuery("types:" + MappedReferenceType.COMMUNITY.getIntValue());
        allCommunityPubs.set("rows", 50);
        assertThat(
                solrClient.query(
                        SolrCollection.publication, allCommunityPubs, PublicationDocument.class),
                hasSize(41));

        List<PublicationDocument> documents =
                solrClient.query(
                        SolrCollection.publication,
                        new SolrQuery("pubmed_id:27190215 AND accession:D4GVJ3"),
                        PublicationDocument.class);

        assertThat(documents, hasSize(1));

        PublicationDocument document = documents.get(0);
        assertThat(document.isLargeScale(), is(true));

        MappedPublications publications = extractObject(document);
        List<CommunityMappedReference> references = publications.getCommunityMappedReferences();
        assertThat(references, hasSize(1));

        CommunityMappedReference reference = references.get(0);

        // ----------- check contents of stored object
        assertThat(reference.getPubMedId(), is("27190215"));
        assertThat(reference.getUniProtKBAccession().getValue(), is("D4GVJ3"));
        assertThat(reference.getSource().getName(), is("ORCID"));
        assertThat(reference.getSource().getId(), is("0000-0001-6105-0923"));
        assertThat(reference.getCommunityAnnotation().getProteinOrGene(), is("HvJAMM2"));
        assertThat(
                reference.getCommunityAnnotation().getFunction(),
                is(
                        "Required for TATA-binding protein 2 (TBP2) turnover by ubiquitin-like proteasome system."));
        assertThat(reference.getSourceCategories(), contains("Function"));
    }
}