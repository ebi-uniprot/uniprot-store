package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 10/05/2020
 */
class InactiveUniProtKBRDDTupleReaderTest {

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

            JavaPairRDD<String, UniProtDocument> uniprotRdd =
                    InactiveUniProtKBRDDTupleReader.load(parameter);
            assertNotNull(uniprotRdd);

            long count = uniprotRdd.count();
            assertEquals(23L, count);
            Tuple2<String, UniProtDocument> inactive =
                    uniprotRdd.filter(tuple2 -> tuple2._1.equals("I8FBX0")).first();
            validateInactiveEntry(inactive, "I8FBX0", "DELETED:UNKNOWN");

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("I8FBX1")).first();
            validateInactiveEntry(inactive, "I8FBX1", "DELETED:PROTEOME_REDUNDANCY");

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("I8FBX2")).first();
            validateInactiveEntry(inactive, "I8FBX2", "DELETED:UNKNOWN");

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("Q00015")).first();
            validateInactiveEntry(inactive, "Q00015", "MERGED:P23141");

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("Q00007")).first();
            validateInactiveEntry(inactive, "Q00007", "DEMERGED:P63150,P63151");
        }
    }

    private static void validateInactiveEntry(
            Tuple2<String, UniProtDocument> deleted, String I8FBX0, String expected) {
        assertNotNull(deleted);
        assertEquals(I8FBX0, deleted._1);
        assertEquals(expected, deleted._2.inactiveReason);
        assertFalse(deleted._2.active);
    }
}
