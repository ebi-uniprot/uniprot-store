package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniref.UniRefLightRDDTupleReader;

import java.util.ResourceBundle;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author sahmad
 * @since 21/08/2020
 */
class ProteomeRDDReaderTest {
    @Test
    void testLoadProteomeWithWithoutPartition() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            ProteomeRDDReader reader =
                    new ProteomeRDDReader(parameter, false);
            JavaPairRDD<String, ProteomeEntry> javaPairRDD = reader.load();
            assertNotNull(javaPairRDD);
            long count = javaPairRDD.count();
            assertEquals(6L, count);
            Tuple2<String, ProteomeEntry> firstTuple = javaPairRDD.first();
            assertNotNull(firstTuple);
            assertEquals("UP000000718", firstTuple._1);
            javaPairRDD.foreach(tuple -> assertEquals(tuple._2.getId().getValue(), tuple._1));
        }
    }
}
