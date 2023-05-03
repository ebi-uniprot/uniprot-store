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
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            ChebiOwlReader chebiOwlReader = new ChebiOwlReader(parameter);
            JavaPairRDD<Long, ChebiEntry> chebiRDD = chebiOwlReader.load();
            List<Tuple2<Long, ChebiEntry>> sampleRows = chebiRDD.take(50);
//            sampleRows.forEach(
//                    entry -> {
//                        System.out.println(entry._1());
//                        System.out.println("Name:" + entry._2().getName());
//                        System.out.println("InchiKey:" + entry._2().getInchiKey());
//                        System.out.println("Synonyms:" + entry._2().getSynonyms());
//                        int relatedIdSize = entry._2().getRelatedIds().size();
//                        int majorMicroSize = entry._2().getMajorMicrospecies().size();
//                        for (int i = 0; i < relatedIdSize; i++) {
//                            if (i == 0) System.out.print("RelatedIds:");
//                            System.out.print((entry._2().getRelatedIds().get(i).getId() + ","));
//                        }
//                        System.out.println();
//                        for (int j = 0; j < majorMicroSize; j++) {
//                            if (j == 0) System.out.print("MajorMicrospecies:");
//                            System.out.print(
//                                    (entry._2().getMajorMicrospecies().get(j).getId() + ","));
//                        }
//                        System.out.println();
//                        System.out.println(
//                                "--------------------------------------------------------");
//                    });
            assertNotNull(chebiRDD);
            long count = chebiRDD.count();
            System.out.print("-----------------------" + count + "-----------------------------");
            assertEquals(40L, count);
//             validateChebiWithMultiplesIsARelations(chebiRDD);
//             validateChebiWithMajorMicroespecies(chebiRDD);
            chebiOwlReader.stopSparkSession();
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

    private void validateChebiWithMultiplesIsARelations(JavaPairRDD<Long, ChebiEntry> chebiRdd) {
        Tuple2<Long, ChebiEntry> tuple =
                chebiRdd.filter(tuple2 -> tuple2._1.equals(16526L)).first();

        assertNotNull(tuple);
        assertEquals(16526L, tuple._1);
        ChebiEntry entry = tuple._2;
        assertEquals("16526", entry.getId());
        assertEquals("carbon dioxide", entry.getName());
        assertEquals("CURLTUGMZLYLDI-UHFFFAOYSA-N", entry.getInchiKey());

        assertNotNull(entry.getSynonyms());
        assertEquals(12, entry.getSynonyms().size());
        assertTrue(entry.getSynonyms().contains("[CO2]"));
        assertTrue(entry.getSynonyms().contains("CARBON DIOXIDE"));
        assertTrue(entry.getSynonyms().contains("carbonic anhydride"));

        assertNotNull(entry.getRelatedIds());
        assertEquals(0, entry.getRelatedIds().size());
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

    private void validateChebiWithMajorMicroespecies(JavaPairRDD<Long, ChebiEntry> chebiRdd) {
        Tuple2<Long, ChebiEntry> tuple =
                chebiRdd.filter(tuple2 -> tuple2._1.equals(4200L)).first();

        assertNotNull(tuple);
        assertEquals(4200L, tuple._1);
        ChebiEntry entry = tuple._2;
        assertEquals("4200", entry.getId());
        assertEquals("4200-major microspecies relation", entry.getName());
        assertEquals("IIHHHHGGGFFFF-AABBBCCCDD-N", entry.getInchiKey());
        assertNotNull(entry.getSynonyms());
        assertTrue(entry.getSynonyms().isEmpty());

        assertNotNull(entry.getRelatedIds());
        assertEquals(14, entry.getRelatedIds().size());
        List<String> relatedIds =
                entry.getRelatedIds().stream().map(ChebiEntry::getId).collect(Collectors.toList());
        assertTrue(relatedIds.contains("138675"));

        // Can Load is_a
        assertTrue(relatedIds.contains("4300"));
        assertTrue(relatedIds.contains("4400"));
        assertTrue(relatedIds.contains("4500"));

        // Can Load has_major_microspecies_at_pH_7_3
        assertTrue(relatedIds.contains("1000"));
        assertTrue(relatedIds.contains("1100"));
        assertTrue(relatedIds.contains("1200"));
        assertTrue(relatedIds.contains("1300"));
        assertTrue(relatedIds.contains("1400"));
        assertTrue(relatedIds.contains("1500"));

        // Can Load has_major_microspecies_at_pH_7_3
        assertTrue(relatedIds.contains("2200"));
        assertTrue(relatedIds.contains("2300"));
        assertTrue(relatedIds.contains("2400"));
        assertTrue(relatedIds.contains("2500"));
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
