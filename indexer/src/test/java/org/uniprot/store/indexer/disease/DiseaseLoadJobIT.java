package org.uniprot.store.indexer.disease;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.cv.disease.CrossReference;
import org.uniprot.core.cv.disease.Disease;
import org.uniprot.core.cv.keyword.Keyword;
import org.uniprot.core.json.parser.disease.DiseaseJsonConfig;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.FakeReadDatabaseConfig;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.disease.DiseaseDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            FakeReadDatabaseConfig.class,
            ListenerConfig.class,
            DiseaseLoadStep.class,
            DiseaseProteinCountStep.class,
            DiseaseLoadJob.class
        })
class DiseaseLoadJobIT {

    private ObjectMapper diseaseObjectMapper =
            DiseaseJsonConfig.getInstance().getFullObjectMapper();

    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrOperations solrOperations;

    @Test
    void testDiseaseLoadJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.DISEASE_LOAD_JOB_NAME));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        StepExecution indexingStep =
                jobExecution.getStepExecutions().stream()
                        .filter(step -> step.getStepName().equals(Constants.DISEASE_INDEX_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(indexingStep.getReadCount(), is(5));
        assertThat(indexingStep.getWriteCount(), is(5));

        Page<DiseaseDocument> response =
                solrOperations.query(
                        SolrCollection.disease.name(),
                        new SimpleQuery("*:*"),
                        DiseaseDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.getTotalElements(), is(5L));

        // get one document
        DiseaseDocument disDoc = response.get().findFirst().get();
        assertThat(disDoc.getAccession(), is("DI-02692"));

        ByteBuffer diseaseByteBuffer = disDoc.getDiseaseObj();

        Assertions.assertNotNull(diseaseByteBuffer);
        // convert the binary to disease object
        Disease disease =
                this.diseaseObjectMapper.readValue(diseaseByteBuffer.array(), Disease.class);
        assertThat(disease.getId(), is("Rheumatoid arthritis"));
        assertThat(disease.getAccession(), is("DI-02692"));
        assertThat(disease.getAcronym(), is("RA"));
        assertThat(disease.getReviewedProteinCount(), is(8L));
        assertThat(disease.getUnreviewedProteinCount(), is(nullValue()));
        assertThat(disease.getCrossReferences().size(), is(3));
        assertThat(disease.getAlternativeNames().size(), is(2));
        assertThat(disease.getKeywords().size(), is(1));
        Assertions.assertTrue(
                disease.getDefinition()
                        .contains(
                                "An inflammatory disease with autoimmune features and a complex genetic"));
        disease.getCrossReferences().forEach(ref -> verifyCrossRef(ref));
        disease.getKeywords().forEach(kw -> verifyKeyword(kw));
        disease.getAlternativeNames().forEach(nm -> assertThat(nm, notNullValue()));
        // clean up
        solrOperations.delete(SolrCollection.disease.name(), new SimpleQuery("*:*"));
        solrOperations.commit(SolrCollection.disease.name());
    }

    private void verifyCrossRef(CrossReference xref) {
        assertThat(xref.getId(), notNullValue());
        assertThat(xref.getDatabaseType(), notNullValue());
        assertThat(xref.getProperties(), notNullValue());
    }

    private void verifyKeyword(Keyword kw) {
        assertThat(kw.getId(), notNullValue());
        assertThat(kw.getAccession(), notNullValue());
    }
}
