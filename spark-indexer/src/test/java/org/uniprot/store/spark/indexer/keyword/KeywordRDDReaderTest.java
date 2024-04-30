package org.uniprot.store.spark.indexer.keyword;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 09/05/2020
 */
class KeywordRDDReaderTest {

    @Test
    void testLoadKeyword() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            KeywordRDDReader reader = new KeywordRDDReader(parameter);
            JavaPairRDD<String, KeywordEntry> keywordRdd = reader.load();
            assertNotNull(keywordRdd);
            long count = keywordRdd.count();
            assertEquals(8L, count);
            Tuple2<String, KeywordEntry> tuple =
                    keywordRdd.filter(tuple2 -> tuple2._1.equals("2fe-2s")).first();

            assertNotNull(tuple);
            assertEquals("2fe-2s", tuple._1);
            assertEquals("KW-0001", tuple._2.getKeyword().getId());
        }
    }
}
