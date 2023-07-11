package org.uniprot.store.spark.indexer.go.evidence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 16/05/2020
 */
class GOEvidencesRDDReaderTest {

    @Test
    void testLoadGOEvidences() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            GOEvidencesRDDReader reader = new GOEvidencesRDDReader(parameter);
            JavaPairRDD<String, Iterable<GOEvidence>> goEvidenceRDD = reader.load();
            assertNotNull(goEvidenceRDD);
            long count = goEvidenceRDD.count();
            assertEquals(4L, count);
            Tuple2<String, Iterable<GOEvidence>> tuple =
                    goEvidenceRDD.filter(tuple2 -> tuple2._1.equals("P21802")).first();

            assertNotNull(tuple);
            assertEquals("P21802", tuple._1);
            List<GOEvidence> goEvidences = new ArrayList<>();
            tuple._2.forEach(goEvidences::add);

            assertEquals(9, goEvidences.size());
            GOEvidence goEvidence = goEvidences.get(0);
            assertEquals("GO:0005007", goEvidence.getGoId());
            assertNotNull(goEvidence.getEvidence());
        }
    }
}
