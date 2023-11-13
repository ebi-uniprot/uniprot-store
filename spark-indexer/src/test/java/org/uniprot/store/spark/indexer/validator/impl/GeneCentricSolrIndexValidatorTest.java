package org.uniprot.store.spark.indexer.validator.impl;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

class GeneCentricSolrIndexValidatorTest {

    @Test
    void getRddCount() {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try(JavaSparkContext context =
                    SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            JobParameter jobParameter = JobParameter.builder()
                    .releaseName("2020_02")
                    .applicationConfig(applicationConfig)
                    .sparkContext(context)
                    .build();

            GeneCentricSolrIndexValidator validator = new GeneCentricSolrIndexValidator(jobParameter);
            assertDoesNotThrow(validator::runValidation);
        }
    }
}