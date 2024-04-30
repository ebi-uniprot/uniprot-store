package org.uniprot.store.spark.indexer.ec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 16/05/2020
 */
class ECRDDReaderTest {

    @Test
    void testLoadEC() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            ECRDDReader reader = new ECRDDReader(parameter);
            JavaPairRDD<String, ECEntry> ecRdd = reader.load();
            assertNotNull(ecRdd);
            long count = ecRdd.count();
            assertEquals(13L, count);
            Tuple2<String, ECEntry> tuple =
                    ecRdd.filter(tuple2 -> tuple2._1.equals("1.-.-.-")).first();

            assertNotNull(tuple);
            assertEquals("1.-.-.-", tuple._1);
            assertEquals("Oxidoreductases", tuple._2.getLabel());
        }
    }
}
