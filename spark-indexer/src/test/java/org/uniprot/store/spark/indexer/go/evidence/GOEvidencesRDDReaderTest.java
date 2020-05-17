package org.uniprot.store.spark.indexer.go.evidence;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 16/05/2020
 */
class GOEvidencesRDDReaderTest {

    @Test
    void testLoadGOEvidences() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            JavaPairRDD<String, Iterable<GOEvidence>> goEvidenceRDD =
                    GOEvidencesRDDReader.load(parameter);
            assertNotNull(goEvidenceRDD);
            long count = goEvidenceRDD.count();
            assertEquals(3L, count);
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
