package org.uniprot.store.spark.indexer.disease;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 10/05/2020
 */
class DiseaseRDDReaderTest {

    @Test
    void testLoadDisease() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            JavaPairRDD<String, DiseaseEntry> diseaseRdd = DiseaseRDDReader.load(parameter);
            assertNotNull(diseaseRdd);
            long count = diseaseRdd.count();
            assertEquals(4L, count);
            Tuple2<String, DiseaseEntry> tuple =
                    diseaseRdd.filter(tuple2 -> tuple2._1.equals("Jackson-Weiss syndrome")).first();

            assertNotNull(tuple);
            assertEquals("Jackson-Weiss syndrome", tuple._1);
            assertEquals("DI-00602", tuple._2.getId());
        }
    }
}
