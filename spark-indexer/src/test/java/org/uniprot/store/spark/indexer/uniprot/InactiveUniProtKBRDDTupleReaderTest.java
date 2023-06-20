package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;

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
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
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
            assertEquals(22L, count);
            Tuple2<String, UniProtDocument> deleted =
                    uniprotRdd.filter(tuple2 -> tuple2._1.equals("I8FBX0")).first();

            assertNotNull(deleted);
            assertEquals("I8FBX0", deleted._1);
            assertEquals("DELETED", deleted._2.inactiveReason);
            assertFalse(deleted._2.active);

            Tuple2<String, UniProtDocument> merged =
                    uniprotRdd.filter(tuple2 -> tuple2._1.equals("Q00015")).first();
            assertNotNull(merged);
            assertEquals("Q00015", merged._1);
            assertEquals("MERGED:P23141", merged._2.inactiveReason);
            assertFalse(merged._2.active);

            Tuple2<String, UniProtDocument> demerged =
                    uniprotRdd.filter(tuple2 -> tuple2._1.equals("Q00007")).first();
            assertNotNull(demerged);
            assertEquals("Q00007", demerged._1);
            assertEquals("DEMERGED:P63150,P63151", demerged._2.inactiveReason);
            assertFalse(demerged._2.active);
        }
    }
}
