package org.uniprot.store.spark.indexer.subcell;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 10/05/2020
 */
class SubcellularLocationRDDReaderTest {

    @Test
    void testLoadSubcellRdd() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            JavaPairRDD<String, SubcellularLocationEntry> subcellRdd =
                    SubcellularLocationRDDReader.load(parameter);
            assertNotNull(subcellRdd);
            long count = subcellRdd.count();
            assertEquals(520L, count);
            Tuple2<String, SubcellularLocationEntry> tuple =
                    subcellRdd.filter(tuple2 -> tuple2._1.equals("acidocalcisome")).first();

            assertNotNull(subcellRdd);
            assertEquals("acidocalcisome", tuple._1);
            assertEquals("SL-0002", tuple._2.getId());
        }
    }
}
