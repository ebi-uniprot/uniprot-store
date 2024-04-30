package org.uniprot.store.spark.indexer.literature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 10/05/2020
 */
class LiteratureMappedRDDReaderTest {

    @Test
    void loadLiteratureMappedReference() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            LiteratureMappedRDDReader reader = new LiteratureMappedRDDReader(parameter);
            JavaPairRDD<String, Iterable<LiteratureMappedReference>> mappedReferenceRdd =
                    reader.load();
            assertNotNull(mappedReferenceRdd);
            long count = mappedReferenceRdd.count();
            assertEquals(5L, count);
            Tuple2<String, Iterable<LiteratureMappedReference>> tuple =
                    mappedReferenceRdd.filter(tuple2 -> tuple2._1.equals("1358782")).first();

            assertNotNull(tuple);
            assertNotNull(tuple._1);
            assertEquals("1358782", tuple._1);

            assertNotNull(tuple._2);
            List<LiteratureMappedReference> mappedReferences = new ArrayList<>();
            tuple._2.forEach(mappedReferences::add);

            assertEquals(11, mappedReferences.size());
            LiteratureMappedReference first = mappedReferences.get(0);
            assertEquals("B5U9V4", first.getUniprotAccession().getValue());
        }
    }

    @Test
    void loadAccessionPubMedRDD() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            LiteratureMappedRDDReader reader = new LiteratureMappedRDDReader(parameter);
            JavaPairRDD<String, Iterable<Tuple2<String, String>>> mappedReferenceRdd =
                    reader.loadAccessionPubMedRDD();
            assertNotNull(mappedReferenceRdd);
            long count = mappedReferenceRdd.count();
            assertEquals(43L, count);
            Tuple2<String, Iterable<Tuple2<String, String>>> tuple =
                    mappedReferenceRdd.filter(tuple2 -> tuple2._1.equals("P38145")).first();

            assertNotNull(tuple);
            assertNotNull(tuple._1);
            assertEquals("P38145", tuple._1);

            assertNotNull(tuple._2);
            List<String> mappedReferences = new ArrayList<>();
            for (Tuple2<String, String> srcPubMedId : tuple._2) {
                if (!"ORCID".equalsIgnoreCase(srcPubMedId._1)) {
                    mappedReferences.add(srcPubMedId._2);
                }
            }
            assertEquals(1, mappedReferences.size());
            assertEquals("5312045", mappedReferences.get(0));
        }
    }
}
