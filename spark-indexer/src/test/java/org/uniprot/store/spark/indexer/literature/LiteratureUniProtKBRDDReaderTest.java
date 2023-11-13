package org.uniprot.store.spark.indexer.literature;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

class LiteratureUniProtKBRDDReaderTest {

    @Test
    void canLoadLiteratureUniProtKBRCitations() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                     SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            LiteratureUniProtKBRDDReader reader = new LiteratureUniProtKBRDDReader(parameter);
            JavaPairRDD<String, LiteratureEntry> result = reader.load();
            assertNotNull(result);
            long count = result.count();
            assertEquals(7L, count);
            validateTuple(result, "CI-73HJSSOHL8LGA");
            validateTuple(result, "21364755");
            validateTuple(result, "15018815");
        }
    }

    private void validateTuple(JavaPairRDD<String, LiteratureEntry> result, String citationId) {
        Tuple2<String, LiteratureEntry> tuple =
                result.filter(tuple2 -> tuple2._1.equals(citationId)).first();

        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertEquals(citationId, tuple._1);

        assertNotNull(tuple._2);
        LiteratureEntry entry = tuple._2;
        assertEquals(citationId, entry.getCitation().getId());
    }
}