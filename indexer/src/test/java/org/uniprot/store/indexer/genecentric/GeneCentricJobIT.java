package org.uniprot.store.indexer.genecentric;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.indexer.common.utils.Constants.GENE_CENTRIC_INDEX_JOB;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.AssertionFailedError;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.fasta.ProteinFasta;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.json.parser.genecentric.GeneCentricJsonConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 03/11/2020
 */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            GeneCentricJob.class,
            GeneCentricCanonicalStep.class,
            GeneCentricRelatedStep.class,
            GeneCentricConfig.class,
            ListenerConfig.class
        })
class GeneCentricJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrOperations;

    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testIndexJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(GENE_CENTRIC_INDEX_JOB));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        List<GeneCentricDocument> response =
                solrOperations.query(
                        SolrCollection.genecentric,
                        new SolrQuery("*:*").setRows(100),
                        GeneCentricDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(40));

        ObjectMapper objectMapper = GeneCentricJsonConfig.getInstance().getFullObjectMapper();

        returnsCanonicalWithoutRelated(objectMapper, response);

        returnsCanonicalWithRelated(objectMapper, response);

        returnCanonicalWithMultipleRelated(objectMapper, response);
    }

    private void returnsCanonicalWithRelated(
            ObjectMapper objectMapper, List<GeneCentricDocument> savedDocuments) {
        GeneCentricDocument document =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().equals("O51971"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals("O51971", document.getAccession());
        assertNotNull(document.getGeneCentricStored());

        GeneCentricEntry entry = getGeneCentricEntry(objectMapper, document.getGeneCentricStored());
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("UP000000554", entry.getProteomeId());
        assertEquals("O51971", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertEquals(1, entry.getRelatedProteins().size());
        Protein relatedProtein = entry.getRelatedProteins().get(0);
        assertEquals("Q9HI14", relatedProtein.getId());
    }

    private void returnCanonicalWithMultipleRelated(
            ObjectMapper objectMapper, List<GeneCentricDocument> savedDocuments) {
        GeneCentricDocument document =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().equals("A0A6G0Z6X6"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals("A0A6G0Z6X6", document.getAccession());
        assertNotNull(document.getGeneCentricStored());

        GeneCentricEntry entry = getGeneCentricEntry(objectMapper, document.getGeneCentricStored());
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("UP000478052", entry.getProteomeId());
        assertEquals("A0A6G0Z6X6", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertEquals(3, entry.getRelatedProteins().size());
        List<String> relatedAccessions =
                entry.getRelatedProteins().stream()
                        .map(ProteinFasta::getId)
                        .collect(Collectors.toList());
        assertTrue(relatedAccessions.contains("A0A6G0Z6T5"));
        assertTrue(relatedAccessions.contains("A0A6G0Z7D3"));
        assertTrue(relatedAccessions.contains("A0A6G0Z7D4"));
    }

    private void returnsCanonicalWithoutRelated(
            ObjectMapper objectMapper, List<GeneCentricDocument> savedDocuments) {
        GeneCentricDocument document =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().equals("A0A6G0ZDD9"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals("A0A6G0ZDD9", document.getAccession());
        assertNotNull(document.getGeneCentricStored());

        GeneCentricEntry entry = getGeneCentricEntry(objectMapper, document.getGeneCentricStored());
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("UP000478052", entry.getProteomeId());
        assertEquals("A0A6G0ZDD9", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertTrue(entry.getRelatedProteins().isEmpty());
    }

    private GeneCentricEntry getGeneCentricEntry(
            ObjectMapper objectMapper, byte[] geneCentricStored) {
        // convert the binary to disease object
        GeneCentricEntry entry = null;
        try {
            entry = objectMapper.readValue(geneCentricStored, GeneCentricEntry.class);
        } catch (IOException e) {
            fail("Unable to parse gene centric json");
        }
        return entry;
    }
}
