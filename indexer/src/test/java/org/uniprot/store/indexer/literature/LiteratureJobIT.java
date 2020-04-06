package org.uniprot.store.indexer.literature;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStoreEntry;
import org.uniprot.core.literature.impl.LiteratureStoreEntryImpl;
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
            LiteratureJobIT.KeywordStatisticsStepFake.class
        })
class LiteratureJobIT {

    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private SolrTemplate template;

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
        assertThat(step.getReadCount(), is(18));
        assertThat(step.getWriteCount(), is(18));

        // Validating if solr document was written correctly
        SimpleQuery solrQuery = new SimpleQuery("*:*");
        solrQuery.addSort(new Sort(Sort.Direction.ASC, "id"));
        Page<LiteratureDocument> response =
                template.query(
                        SolrCollection.literature.name(), solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(18L));

        // Validating if solr document was written correctly
        solrQuery = new SimpleQuery("author:Hendrickson W.A.");
        solrQuery.addSort(new Sort(Sort.Direction.ASC, "id"));
        response =
                template.query(
                        SolrCollection.literature.name(), solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(1L));

        // Validating if solr document was written correctly
        solrQuery =
                new SimpleQuery(
                        "title:Glutamine synthetase in newborn mice homozygous for lethal albino alleles");
        solrQuery.addSort(new Sort(Sort.Direction.ASC, "id"));
        response =
                template.query(
                        SolrCollection.literature.name(), solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(1L));

        // Validating if solr document was written correctly
        solrQuery = new SimpleQuery("mapped_protein:Q15423");
        response =
                template.query(
                        SolrCollection.literature.name(), solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(1L));

        // validating if can search one single entry with mapped and cited items
        solrQuery = new SimpleQuery("id:11203701");
        response =
                template.query(
                        SolrCollection.literature.name(), solrQuery, LiteratureDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(1L));

        LiteratureDocument literatureDocument = response.getContent().get(0);
        validateLiteratureDocument(literatureDocument);

        ByteBuffer byteBuffer = literatureDocument.getLiteratureObj();
        ObjectMapper jsonMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
        LiteratureStoreEntry storeEntry =
                jsonMapper.readValue(byteBuffer.array(), LiteratureStoreEntryImpl.class);
        assertThat(storeEntry.hasLiteratureEntry(), is(true));
        validateLiteratureEntry(storeEntry.getLiteratureEntry());
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
        assertThat(entry.getStatistics().getMappedProteinCount(), is(19L));
        // assertThat(entry.getStatistics().getReviewedProteinCount(), is(1L)); COVID-19 CHANGE DO
        // NOT MERGE
        // assertThat(entry.getStatistics().getUnreviewedProteinCount(), is(1L)); COVID-19 CHANGE DO
        // NOT MERGE
    }

    private void validateLiteratureDocument(LiteratureDocument literatureDocument) {
        assertThat(literatureDocument, is(notNullValue()));
        assertThat(literatureDocument.getId(), is(notNullValue()));
        assertThat(literatureDocument.getId(), is("11203701"));
        assertThat(literatureDocument.getDoi(), is("10.1006/dbio.2000.9955"));
        // assertThat(literatureDocument.isCitedin(), is(true)); COVID-19 CHANGE DO NOT MERGE
        assertThat(literatureDocument.isMappedin(), is(true));
        assertThat(literatureDocument.getLiteratureObj(), is(notNullValue()));
    }

    @Configuration
    static class KeywordStatisticsStepFake extends LiteratureStatisticsStep {

        @Override
        protected String getStatisticsSQL() {
            return LiteratureSQLConstants.LITERATURE_STATISTICS_SQL.replaceAll(
                    "FULL JOIN", "INNER JOIN");
        }
    }
}
