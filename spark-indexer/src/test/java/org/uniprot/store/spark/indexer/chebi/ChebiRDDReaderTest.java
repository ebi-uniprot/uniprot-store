package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 10/05/2020
 */
class ChebiRDDReaderTest {

    @Test
    void testLoadChebi() {
        // CHEBI:156068
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            ChebiRDDReader reader = new ChebiRDDReader(parameter);
            JavaPairRDD<String, ChebiEntry> chebiRdd = reader.load();
            assertNotNull(chebiRdd);
            long count = chebiRdd.count();
            assertEquals(31L, count);
            // 16526
            validateChebiWithMultiplesIsARelationAndMicrospeciesRelations(chebiRdd);

            // 3200 And 6500
            validateChebiWithIsARelationAndConjugateRelations(chebiRdd);

            // 5500 -> has relation with 5600 and 5700
            validateChebiWithTSVPH7Relations(chebiRdd);
        }
    }

    @Test
    void testLoadGraphThrowingMaxCycleException() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            ChebiRDDReader reader = new ChebiRDDReader(parameter);
            JavaRDD<Tuple2<Object, ChebiEntry>> vertices = loadVertices(parameter);
            SparkIndexException exception =
                    assertThrows(
                            SparkIndexException.class, () -> reader.loadChebiGraph(vertices, 6));
            assertNotNull(exception);
        }
    }

    private void validateChebiWithMultiplesIsARelationAndMicrospeciesRelations(
            JavaPairRDD<String, ChebiEntry> chebiRdd) {
        Tuple2<String, ChebiEntry> tuple =
                chebiRdd.filter(tuple2 -> tuple2._1.equals("16526")).first();

        assertNotNull(tuple);
        assertEquals("16526", tuple._1);
        ChebiEntry entry = tuple._2;
        assertEquals("16526", entry.getId());
        assertEquals("carbon dioxide", entry.getName());
        assertEquals("CURLTUGMZLYLDI-UHFFFAOYSA-N", entry.getInchiKey());

        assertNotNull(entry.getSynonyms());
        assertEquals(11, entry.getSynonyms().size());
        assertTrue(entry.getSynonyms().contains("[CO2]"));
        assertTrue(entry.getSynonyms().contains("CARBON DIOXIDE"));
        assertTrue(entry.getSynonyms().contains("carbonic anhydride"));

        assertNotNull(entry.getRelatedIds());
        assertEquals(15, entry.getRelatedIds().size());
        List<String> relatedIds =
                entry.getRelatedIds().stream().map(ChebiEntry::getId).collect(Collectors.toList());
        assertTrue(relatedIds.contains("138675"));
        // Can Load is_a
        assertTrue(relatedIds.contains("1000"));
        assertTrue(relatedIds.contains("1100"));
        assertTrue(relatedIds.contains("1200"));
        assertTrue(relatedIds.contains("1300"));
        assertTrue(relatedIds.contains("1400"));
        assertTrue(relatedIds.contains("1500"));

        // Can Load is_a
        assertTrue(relatedIds.contains("2200"));
        assertTrue(relatedIds.contains("2300"));
        assertTrue(relatedIds.contains("2400"));
        assertTrue(relatedIds.contains("2500"));

        // Can Load has_major_microspecies_at_pH_7_3
        assertTrue(relatedIds.contains("4200"));
        assertTrue(relatedIds.contains("4300"));
        assertTrue(relatedIds.contains("4400"));
        assertTrue(relatedIds.contains("4500"));
    }

    private void validateChebiWithIsARelationAndConjugateRelations(
            JavaPairRDD<String, ChebiEntry> chebiRdd) {
        Tuple2<String, ChebiEntry> tuple =
                chebiRdd.filter(tuple2 -> tuple2._1.equals("3200")).first();

        assertNotNull(tuple);
        assertEquals("3200", tuple._1);
        ChebiEntry entry = tuple._2;
        assertEquals("3200", entry.getId());
        assertEquals("3200-conjugate-base-relation", entry.getName());
        assertNull(entry.getInchiKey());

        assertNotNull(entry.getRelatedIds());
        assertEquals(6, entry.getRelatedIds().size());
        List<String> relatedIds =
                entry.getRelatedIds().stream().map(ChebiEntry::getId).collect(Collectors.toList());
        // Can Load is_a
        assertTrue(relatedIds.contains("3300"));
        assertTrue(relatedIds.contains("3400"));
        assertTrue(relatedIds.contains("3500"));

        // Can Load is_conjugate_base_of
        assertTrue(relatedIds.contains("6500"));
        assertTrue(relatedIds.contains("6600"));
        assertTrue(relatedIds.contains("6700"));

        tuple = chebiRdd.filter(tuple2 -> tuple2._1.equals("6500")).first();
        entry = tuple._2;
        assertEquals("6500", entry.getId());
        assertEquals("6500-conjugate-acid-relation-chain", entry.getName());
        assertNull(entry.getInchiKey());

        assertNotNull(entry.getRelatedIds());
        assertEquals(6, entry.getRelatedIds().size());
        relatedIds =
                entry.getRelatedIds().stream().map(ChebiEntry::getId).collect(Collectors.toList());

        // Can Load is_conjugate_acid_of
        assertTrue(relatedIds.contains("3200"));
        assertTrue(relatedIds.contains("3300"));
        assertTrue(relatedIds.contains("3400"));
        assertTrue(relatedIds.contains("3500"));

        // Can Load is_a
        assertTrue(relatedIds.contains("6600"));
        assertTrue(relatedIds.contains("6700"));
    }

    private void validateChebiWithTSVPH7Relations(JavaPairRDD<String, ChebiEntry> chebiRdd) {
        Tuple2<String, ChebiEntry> tuple =
                chebiRdd.filter(tuple2 -> tuple2._1.equals("5500")).first();

        assertNotNull(tuple);
        assertEquals("5500", tuple._1);
        ChebiEntry entry = tuple._2;
        assertEquals("5500", entry.getId());
        assertEquals("5500-ph7-tsv-chebi-mapping", entry.getName());
        assertNull(entry.getInchiKey());

        assertNotNull(entry.getRelatedIds());
        assertEquals(2, entry.getRelatedIds().size());
        List<String> relatedIds =
                entry.getRelatedIds().stream().map(ChebiEntry::getId).collect(Collectors.toList());
        // Can Load TSV PH7 related ids
        assertTrue(relatedIds.contains("5600"));
        assertTrue(relatedIds.contains("5700"));
    }

    private static JavaRDD<Tuple2<Object, ChebiEntry>> loadVertices(JobParameter parameter) {
        List<Tuple2<Object, ChebiEntry>> vertices = new ArrayList<>();
        ChebiEntry currentEntry = new ChebiEntryBuilder().id("1").build();
        vertices.add(new Tuple2<>(Long.parseLong(currentEntry.getId()), currentEntry));
        for (int i = 2; i < 70; i++) {
            ChebiEntry related = new ChebiEntryBuilder().id(currentEntry.getId()).build();
            ChebiEntry entry = new ChebiEntryBuilder().id("" + i).relatedIdsAdd(related).build();
            vertices.add(new Tuple2<>(Long.parseLong(entry.getId()), entry));
            currentEntry = entry;
        }

        return parameter.getSparkContext().parallelize(vertices);
    }
}
