package org.uniprot.store.spark.indexer.subcellularlocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.json.parser.subcell.SubcellularLocationJsonConfig;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

/**
 * @author sahmad
 * @since 02/02/2022
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SubcellularLocationDocumentsToHDFSWriterTest {

    private JobParameter parameter;

    @BeforeAll
    void setUpWriter() {
        Config application = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application);
        parameter =
                JobParameter.builder()
                        .applicationConfig(application)
                        .releaseName("2020_02")
                        .sparkContext(sparkContext)
                        .build();
    }

    @AfterAll
    void closeWriter() {
        parameter.getSparkContext().close();
    }

    @Test
    void canIndexSubcellularLocation() throws IOException {
        SubcellularLocationDocumentsToHDFSWriterTest.SubcellularLocationDocumentsToHDFSWriterFake
                writer = new SubcellularLocationDocumentsToHDFSWriterFake(parameter);
        writer.writeIndexDocumentsToHDFS();
        List<SubcellularLocationDocument> savedDocs = writer.getSavedDocuments();
        assertNotNull(savedDocs);
        assertEquals(520, savedDocs.size());
        SubcellularLocationDocument membraneDoc =
                savedDocs.stream()
                        .filter(d -> d.getName().equalsIgnoreCase("membrane"))
                        .findFirst()
                        .get();
        assertNotNull(membraneDoc);
        SubcellularLocationEntry membraneEntry = extractEntryFromDocument(membraneDoc);
        assertNotNull(membraneEntry);
        assertEquals("Membrane", membraneEntry.getName());
        assertNotNull(membraneEntry.getStatistics());
        assertEquals(1L, membraneEntry.getStatistics().getReviewedProteinCount());
        assertEquals(0L, membraneEntry.getStatistics().getUnreviewedProteinCount());
    }

    SubcellularLocationEntry extractEntryFromDocument(SubcellularLocationDocument document)
            throws IOException {
        ObjectMapper objectMapper =
                SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
        return objectMapper.readValue(
                document.getSubcellularlocationObj(), SubcellularLocationEntry.class);
    }

    private static class SubcellularLocationDocumentsToHDFSWriterFake
            extends SubcellularLocationDocumentsToHDFSWriter {
        private List<SubcellularLocationDocument> documents;

        public SubcellularLocationDocumentsToHDFSWriterFake(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        void saveToHDFS(JavaRDD<SubcellularLocationDocument> subcellularLocationDocumentRDD) {
            this.documents = subcellularLocationDocumentRDD.collect();
        }

        List<SubcellularLocationDocument> getSavedDocuments() {
            return this.documents;
        }
    }
}
