package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 09/05/2020
 */
class UniProtKBRDDTupleReaderTest {

    @Test
    void testLoadUniProtKB() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, true);
            JavaPairRDD<String, UniProtKBEntry> uniprotRdd = reader.load();
            assertNotNull(uniprotRdd);

            assertEquals(1L, uniprotRdd.count());
            Tuple2<String, UniProtKBEntry> tuple =
                    uniprotRdd.filter(tuple2 -> tuple2._1.equals("Q9EPI6")).first();

            assertNotNull(tuple);
            assertEquals("Q9EPI6", tuple._1);
            assertEquals("NSMF_RAT", tuple._2.getUniProtkbId().getValue());
        }
    }
}
