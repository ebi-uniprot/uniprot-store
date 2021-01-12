package org.uniprot.store.indexer.literature;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.uniprot.core.citation.Literature;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.impl.LiteratureEntryImpl;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.literature.steps.LiteratureLoadStep;
import org.uniprot.store.indexer.literature.steps.LiteratureMappingStep;
import org.uniprot.store.indexer.literature.steps.LiteratureStatisticsStep;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.FakeReadDatabaseConfig;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            FakeReadDatabaseConfig.class,
            ListenerConfig.class,
            LiteratureJob.class,
            LiteratureLoadStep.class,
            LiteratureMappingStep.class,
            LiteratureStatisticsStep.class
        })
class LiteratureJobIT {

    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testLiteratureIndexingJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.LITERATURE_LOAD_JOB_NAME));

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
        assertThat(stepMap.containsKey(Constants.LITERATURE_INDEX_STEP), is(true));
        StepExecution step = stepMap.get(Constants.LITERATURE_INDEX_STEP);
        assertThat(step.getReadCount(), is(19));
        assertThat(step.getWriteCount(), is(19));

        // Validating if solr document was written correctly
        SolrQuery solrQuery = new SolrQuery("*:*").setRows(20);
        solrQuery.addSort(SolrQuery.SortClause.asc("id"));
        List<LiteratureDocument> response =
                solrClient.query(SolrCollection.literature, solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(19));

        // Validating if solr document was written correctly
        solrQuery = new SolrQuery("author:\"Hendrickson W.A.\"");
        solrQuery.addSort(SolrQuery.SortClause.asc("id"));
        response = solrClient.query(SolrCollection.literature, solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));

        // Validating if solr document was written correctly
        solrQuery =
                new SolrQuery(
                        "title:\"Glutamine synthetase in newborn mice homozygous for lethal albino alleles\"");
        solrQuery.addSort(SolrQuery.SortClause.asc("id"));
        response = solrClient.query(SolrCollection.literature, solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));

        // validating if can search one single entry with mapped and cited items
        solrQuery = new SolrQuery("id:11203701");
        response = solrClient.query(SolrCollection.literature, solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));

        LiteratureDocument literatureDocument = response.get(0);
        validateLiteratureDocument(literatureDocument);

        ByteBuffer byteBuffer = literatureDocument.getLiteratureObj();
        ObjectMapper jsonMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
        LiteratureEntry storeEntry =
                jsonMapper.readValue(byteBuffer.array(), LiteratureEntryImpl.class);
        assertThat(storeEntry, is(notNullValue()));
        validateLiteratureEntry(storeEntry);
        validateWithCommunityReference();
        verifySearchInAbstract();
        verifySearchInAuthoringGroup();
    }

    private void validateLiteratureEntry(LiteratureEntry entry) {
        assertThat(entry, is(notNullValue()));
        assertTrue(entry.hasCitation());
        Literature literature = (Literature) entry.getCitation();

        assertThat(literature.hasPubmedId(), is(true));
        assertThat(literature.getPubmedId(), is(11203701L));

        assertThat(literature.hasDoiId(), is(true));
        assertThat(literature.getDoiId(), is("10.1006/dbio.2000.9955"));

        assertThat(literature.hasTitle(), is(true));
        assertThat(
                literature.getTitle(),
                is(
                        "TNF signaling via the ligand-receptor pair ectodysplasin "
                                + "and edar controls the function of epithelial signaling centers and is regulated by Wnt "
                                + "and activin during tooth organogenesis."));

        assertThat(literature.hasAuthoringGroup(), is(false));

        assertThat(literature.hasAuthors(), is(true));
        assertThat(literature.getAuthors().size(), is(10));

        assertThat(literature.isCompleteAuthorList(), is(true));

        assertThat(literature.hasPublicationDate(), is(true));
        assertThat(literature.getPublicationDate().getValue(), is("2001"));

        assertThat(literature.hasJournal(), is(true));
        assertThat(literature.getJournal().getName(), is("Dev. Biol."));

        assertThat(literature.hasFirstPage(), is(true));
        assertThat(literature.getFirstPage(), is("443"));

        assertThat(literature.hasLastPage(), is(true));
        assertThat(literature.getLastPage(), is("455"));

        assertThat(literature.hasVolume(), is(true));
        assertThat(literature.getVolume(), is("229"));

        assertThat(entry.hasStatistics(), is(true));
        assertThat(entry.getStatistics().getComputationallyMappedProteinCount(), is(19L));
        assertThat(entry.getStatistics().getCommunityMappedProteinCount(), is(0L));
        assertThat(entry.getStatistics().getReviewedProteinCount(), is(1L));
        assertThat(entry.getStatistics().getUnreviewedProteinCount(), is(1L));
    }

    private void validateLiteratureDocument(LiteratureDocument literatureDocument) {
        assertThat(literatureDocument, is(notNullValue()));
        assertThat(literatureDocument.getId(), is(notNullValue()));
        assertThat(literatureDocument.getId(), is("11203701"));
        assertThat(literatureDocument.getDoi(), is("10.1006/dbio.2000.9955"));
        // assertThat(literatureDocument.isCitedin(), is(true)); COVID-19 CHANGE DO NOT MERGE
        assertThat(literatureDocument.isComputationalMapped(), is(true));
        assertThat(literatureDocument.isCommunityMapped(), is(false));
        assertThat(literatureDocument.getLiteratureObj(), is(notNullValue()));
    }

    private void validateWithCommunityReference() throws IOException {
        SolrQuery solrQuery = new SolrQuery("id:28751710");
        List<LiteratureDocument> response =
                solrClient.query(SolrCollection.literature, solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));
        LiteratureDocument literatureDocument = response.get(0);
        assertThat(literatureDocument.isComputationalMapped(), is(true));
        assertThat(literatureDocument.isCommunityMapped(), is(true));
        assertThat(literatureDocument.isUniprotkbMapped(), is(false));
        ByteBuffer byteBuffer = literatureDocument.getLiteratureObj();
        ObjectMapper jsonMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
        LiteratureEntry storeEntry =
                jsonMapper.readValue(byteBuffer.array(), LiteratureEntryImpl.class);
        assertThat(storeEntry, is(notNullValue()));
        Literature literature = (Literature) storeEntry.getCitation();
        assertThat(literature.getPubmedId(), is(28751710L));
        assertThat(storeEntry.hasStatistics(), is(true));
        assertThat(storeEntry.getStatistics().getComputationallyMappedProteinCount(), is(9L));
        assertThat(storeEntry.getStatistics().getCommunityMappedProteinCount(), is(2L));
        assertThat(storeEntry.getStatistics().getReviewedProteinCount(), is(0L));
        assertThat(storeEntry.getStatistics().getUnreviewedProteinCount(), is(0L));
    }

    private void verifySearchInAbstract(){
        SolrQuery solrQuery = new SolrQuery("lit_abstract:carboxylate");
        List<LiteratureDocument> response =
                solrClient.query(SolrCollection.literature, solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));
        LiteratureDocument literatureDocument = response.get(0);
        assertThat(literatureDocument.getId(), is("44"));
    }

    private void verifySearchInAuthoringGroup(){
        SolrQuery solrQuery = new SolrQuery("author_group:\"United Kingdom\"");
        List<LiteratureDocument> response =
                solrClient.query(SolrCollection.literature, solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(1));
        LiteratureDocument literatureDocument = response.get(0);
        assertThat(literatureDocument.getId(), is("16325696"));
    }
}
