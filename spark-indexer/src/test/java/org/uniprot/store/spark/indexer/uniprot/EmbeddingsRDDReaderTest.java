package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.ConfigFactory;

class EmbeddingsRDDReaderTest {

    @Test
    void readsAllParquetPartsAsAccessionVectors() throws Exception {
        var path = "src/test/resources/2020_02/embeddings";
        var config =
                ConfigFactory.parseString(
                        "embeddings.file.path=\"" + path + "\"\nembeddings.dimension=2");
        var sparkConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext context = SparkUtils.loadSparkContext(sparkConfig, SPARK_LOCAL_MASTER);
        try {
            JobParameter parameter =
                    JobParameter.builder().applicationConfig(config).sparkContext(context).build();
            Map<String, List<Float>> result =
                    new EmbeddingsRDDReader(parameter).load().collectAsMap();
            assertEquals(Map.of("P1", List.of(1.0f, 2.0f), "P2", List.of(3.0f, 4.0f)), result);
        } finally {
            context.close();
        }
    }
}
