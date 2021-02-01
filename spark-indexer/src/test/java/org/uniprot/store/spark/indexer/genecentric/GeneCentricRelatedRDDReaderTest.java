package org.uniprot.store.spark.indexer.genecentric;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
class GeneCentricRelatedRDDReaderTest {

    @Test
    void testLoadGeneCentricRelatedProteins() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            GeneCentricRelatedRDDReader reader = new GeneCentricRelatedRDDReader(parameter);
            JavaPairRDD<String, GeneCentricEntry> uniprotRdd = reader.load();
            assertNotNull(uniprotRdd);

            assertEquals(10L, uniprotRdd.count());

            // can load from UP000000554 fasta file
            canLoadUP000000554RelatedFile(uniprotRdd);

            // can load from UP000478052 fasta file
            canLoadUP000478052RelatedFile(uniprotRdd);
        }
    }

    private void canLoadUP000478052RelatedFile(JavaPairRDD<String, GeneCentricEntry> uniprotRdd) {
        Tuple2<String, GeneCentricEntry> tuple =
                uniprotRdd.filter(tuple2 -> tuple2._1.equals("A0A6G0Z794")).first();

        assertNotNull(tuple);
        assertEquals("A0A6G0Z794", tuple._1);
        GeneCentricEntry entry = tuple._2;
        assertNotNull(entry);
        assertEquals("UP000478052", entry.getProteomeId());
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("A0A6G0Z794", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertFalse(entry.getRelatedProteins().isEmpty());
        Protein relatedProtein = entry.getRelatedProteins().get(0);
        assertNotNull(relatedProtein);
        assertEquals("A0A6G0Z7U2", relatedProtein.getId());
        assertEquals("FWK35_00002192", relatedProtein.getGeneName());
    }

    private void canLoadUP000000554RelatedFile(JavaPairRDD<String, GeneCentricEntry> uniprotRdd) {
        Tuple2<String, GeneCentricEntry> tuple =
                uniprotRdd.filter(tuple2 -> tuple2._1.equals("P0CX05")).first();

        assertNotNull(tuple);
        assertEquals("P0CX05", tuple._1);
        GeneCentricEntry entry = tuple._2;
        assertNotNull(entry);
        assertEquals("UP000000554", entry.getProteomeId());
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("P0CX05", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertFalse(entry.getRelatedProteins().isEmpty());
        Protein relatedProtein = entry.getRelatedProteins().get(0);
        assertNotNull(relatedProtein);
        assertEquals("Q9HPK7", relatedProtein.getId());
        assertEquals("DDE_Tnp_1 domain-containing protein", relatedProtein.getProteinName());
    }
}
