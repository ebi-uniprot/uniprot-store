package org.uniprot.store.spark.indexer.subcell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 10/05/2020
 */
class SubcellularLocationRDDReaderTest {

    @Test
    void testLoadSubcellRdd() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            SubcellularLocationRDDReader reader = new SubcellularLocationRDDReader(parameter);
            JavaPairRDD<String, SubcellularLocationEntry> subcellRdd = reader.load();
            assertNotNull(subcellRdd);
            long count = subcellRdd.count();
            assertEquals(520L, count);
            Tuple2<String, SubcellularLocationEntry> tuple =
                    subcellRdd.filter(tuple2 -> tuple2._1.equals("SL-0002")).first();

            assertNotNull(subcellRdd);
            assertEquals("SL-0002", tuple._1);
            assertEquals("SL-0002", tuple._2.getId());
            assertEquals("Acidocalcisome", tuple._2.getName());
        }
    }
}
