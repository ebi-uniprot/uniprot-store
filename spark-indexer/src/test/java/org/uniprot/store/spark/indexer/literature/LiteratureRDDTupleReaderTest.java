package org.uniprot.store.spark.indexer.literature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.Literature;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 31/03/2021
 */
class LiteratureRDDTupleReaderTest {

    @Test
    void canLoadLiteratureRDD() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            LiteratureRDDTupleReader reader = new LiteratureRDDTupleReader(parameter);
            JavaPairRDD<String, Literature> result = reader.load();
            assertNotNull(result);
            long count = result.count();
            assertEquals(10L, count);
            Tuple2<String, Literature> tuple =
                    result.filter(tuple2 -> tuple2._1.equals("357")).first();

            assertNotNull(tuple);
            assertNotNull(tuple._1);
            assertEquals("357", tuple._1);

            assertNotNull(tuple._2);
            Literature entry = tuple._2;
            assertEquals("357", entry.getId());
            assertEquals(
                    "Novel type of murein transglycosylase in Escherichia coli.", entry.getTitle());
        }
    }
}
