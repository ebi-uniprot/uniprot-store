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
            assertEquals(17L, count);
            Tuple2<String, ChebiEntry> tuple =
                    chebiRdd.filter(tuple2 -> tuple2._1.equals("16526")).first();

            assertNotNull(tuple);
            assertEquals("16526", tuple._1);
            ChebiEntry entry = tuple._2;
            assertEquals("16526", entry.getId());
            assertEquals("carbon dioxide", entry.getName());
            assertEquals("CURLTUGMZLYLDI-UHFFFAOYSA-N", entry.getInchiKey());

            assertNotNull(entry.getRelatedIds());
            assertEquals(11, entry.getRelatedIds().size());
            List<String> relatedIds =
                    entry.getRelatedIds().stream()
                            .map(ChebiEntry::getId)
                            .collect(Collectors.toList());
            assertTrue(relatedIds.contains("138675"));

            assertTrue(relatedIds.contains("1000"));
            assertTrue(relatedIds.contains("1100"));
            assertTrue(relatedIds.contains("1200"));
            assertTrue(relatedIds.contains("1300"));
            assertTrue(relatedIds.contains("1400"));
            assertTrue(relatedIds.contains("1500"));

            assertTrue(relatedIds.contains("2200"));
            assertTrue(relatedIds.contains("2300"));
            assertTrue(relatedIds.contains("2400"));
            assertTrue(relatedIds.contains("2500"));
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
