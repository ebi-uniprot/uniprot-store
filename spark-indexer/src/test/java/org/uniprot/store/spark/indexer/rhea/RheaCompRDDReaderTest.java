package org.uniprot.store.spark.indexer.rhea;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.rhea.model.RheaComp;

import scala.Tuple2;

import com.typesafe.config.Config;

class RheaCompRDDReaderTest {

    @Test
    void canLoadRheaComp() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            RheaCompRDDReader reader = new RheaCompRDDReader(parameter);
            JavaPairRDD<String, RheaComp> rheaCompRDD = reader.load();
            assertNotNull(rheaCompRDD);
            long count = rheaCompRDD.count();
            assertEquals(10L, count);

            // RHEA-COMP:12851
            Tuple2<String, RheaComp> tuple =
                    rheaCompRDD.filter(tuple2 -> tuple2._1.equals("RHEA-COMP:12851")).first();
            assertEquals("RHEA-COMP:12851", tuple._1);
            assertNotNull(tuple._2);
            RheaComp rheaComp = tuple._2;
            assertEquals("RHEA-COMP:12851", rheaComp.getId());
            assertEquals("cytidine(32) in tRNA(Ser)", rheaComp.getName());
        }
    }
}
