package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
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

            JavaPairRDD<String, ChebiEntry> chebiRdd = ChebiRDDReader.load(parameter);
            assertNotNull(chebiRdd);
            long count = chebiRdd.count();
            assertEquals(4L, count);
            Tuple2<String, ChebiEntry> tuple =
                    chebiRdd.filter(tuple2 -> tuple2._1.equals("24431")).first();

            assertNotNull(tuple);
            assertEquals("24431", tuple._1);
            assertEquals("24431", tuple._2.getId());
        }
    }
}
