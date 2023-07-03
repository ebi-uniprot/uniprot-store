package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

import com.typesafe.config.Config;

class UniProtIdTrackerRDDReaderTest {

    @Test
    void load() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            UniProtIdTrackerRDDReader reader = new UniProtIdTrackerRDDReader(parameter);
            JavaPairRDD<String, Set<String>> result = reader.load();
            assertNotNull(result);

            long count = result.count();
            assertEquals(8L, count);

            Tuple2<String, Set<String>> entry =
                    result.filter(tuple2 -> tuple2._1.equals("A0A009IHW8")).first();
            assertNotNull(entry);
            assertEquals("A0A009IHW8", entry._1);
            assertEquals(1, entry._2.size());
            assertTrue(entry._2.contains("ABTIR_ACIBA"));

            entry = result.filter(tuple2 -> tuple2._1.equals("A0A011QK89")).first();
            assertNotNull(entry);
            assertEquals("A0A011QK89", entry._1);
            assertEquals(2, entry._2.size());
            assertTrue(entry._2.containsAll(Set.of("L2HDH_ACCPH", "L2HDH_ACCSB")));

            entry = result.filter(tuple2 -> tuple2._1.equals("Q9EPI6")).first();
            assertNotNull(entry);
            assertEquals("Q9EPI6", entry._1);
            assertEquals(3, entry._2.size());
            assertTrue(
                    entry._2.containsAll(Set.of("NSMFOLD1_RAT", "NSMFOLD2_RAT", "NSMFOLD3_RAT")));
        }
    }
}
