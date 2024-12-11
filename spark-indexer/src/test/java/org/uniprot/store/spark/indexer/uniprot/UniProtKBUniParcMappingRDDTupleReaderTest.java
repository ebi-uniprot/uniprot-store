package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

class UniProtKBUniParcMappingRDDTupleReaderTest {
    @Test
    void testLoadAllUniProtKB() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniProtKBUniParcMappingRDDTupleReader reader =
                    new UniProtKBUniParcMappingRDDTupleReader(parameter);
            JavaPairRDD<String, String> uniProtKBMapperRdd = reader.load();
            assertNotNull(uniProtKBMapperRdd);

            assertEquals(7L, uniProtKBMapperRdd.count());
            validateUniProtKB(uniProtKBMapperRdd, "Q9EPI6", "UPI00000E8551");
            validateUniProtKB(uniProtKBMapperRdd, "Q00007", "UPI000000017F");
        }
    }

    @Test
    void testLoadActiveUniProtKB() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniProtKBUniParcMappingRDDTupleReader reader =
                    new UniProtKBUniParcMappingRDDTupleReader(parameter, true);
            JavaPairRDD<String, String> uniProtKBMapperRdd = reader.load();
            assertNotNull(uniProtKBMapperRdd);

            assertEquals(3L, uniProtKBMapperRdd.count());
            validateUniProtKB(uniProtKBMapperRdd, "Q9EPI6", "UPI00000E8551");
        }
    }

    @Test
    void testLoadInactiveUniProtKB() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniProtKBUniParcMappingRDDTupleReader reader =
                    new UniProtKBUniParcMappingRDDTupleReader(parameter, false);
            JavaPairRDD<String, String> uniProtKBMapperRdd = reader.load();
            assertNotNull(uniProtKBMapperRdd);

            assertEquals(4L, uniProtKBMapperRdd.count());
            validateUniProtKB(uniProtKBMapperRdd, "Q00007", "UPI000000017F");
        }
    }

    private static void validateUniProtKB(
            JavaPairRDD<String, String> uniProtKBMapperRdd, String accession, String uniParcId) {
        Tuple2<String, String> tuple =
                uniProtKBMapperRdd.filter(tuple2 -> tuple2._1.equals(accession)).first();

        assertNotNull(tuple);
        assertEquals(accession, tuple._1);
        assertEquals(uniParcId, tuple._2);
    }
}
