package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

class UniParcSequenceSourceMapperRDDTupleReaderTest {

    @Test
    void testLoadUniParcSource() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniParcSequenceSourceMapperRDDTupleReader reader =
                    new UniParcSequenceSourceMapperRDDTupleReader(parameter);
            JavaPairRDD<String, Set<String>> result = reader.load();
            assertNotNull(result);
            Tuple2<String, Set<String>> entry =
                    result.filter(tuple2 -> tuple2._1.equals("O68891")).first();
            assertNotNull(entry);
            assertEquals("O68891", entry._1);
            assertEquals(2, entry._2.size());
            assertTrue(entry._2.containsAll(Set.of("AAC13493", "AAC13494")));

            entry = result.filter(tuple2 -> tuple2._1.equals("I8FBX0")).first();
            assertNotNull(entry);
            assertEquals("I8FBX0", entry._1);
            assertEquals(1, entry._2.size());
            assertTrue(entry._2.containsAll(Set.of("CAC20866")));
        }
    }
}
