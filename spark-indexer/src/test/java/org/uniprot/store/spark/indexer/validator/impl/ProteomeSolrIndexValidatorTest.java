package org.uniprot.store.spark.indexer.validator.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;
import static org.uniprot.store.spark.indexer.validator.impl.SolrIndexValidatorUtil.getJobParameter;
import static org.uniprot.store.spark.indexer.validator.impl.SolrIndexValidatorUtil.spyMockValidator;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

class ProteomeSolrIndexValidatorTest {

    @Test
    void runValidationSuccess() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            ProteomeSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 7L, 7L);

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalidDocument() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            ProteomeSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 6L, 7L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: proteome, DocumentOutput COUNT: 6, RDD COUNT: 7, Solr COUNT: 7",
                    error.getMessage());
        }
    }

    @Test
    void runValidationInvalidSolr() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            ProteomeSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 7L, 6L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: proteome, DocumentOutput COUNT: 7, RDD COUNT: 7, Solr COUNT: 6",
                    error.getMessage());
        }
    }

    private ProteomeSolrIndexValidator prepareValidator(
            Config applicationConfig, JavaSparkContext context, long docCount, long solrCount)
            throws Exception {
        JobParameter jobParameter = getJobParameter(applicationConfig, context);
        return spyMockValidator(new ProteomeSolrIndexValidator(jobParameter), docCount, solrCount);
    }
}
