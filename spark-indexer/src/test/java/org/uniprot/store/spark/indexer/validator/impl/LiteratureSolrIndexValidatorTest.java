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

class LiteratureSolrIndexValidatorTest {

    @Test
    void runValidationSuccess() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            LiteratureSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 15L, 15L);

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalidDocument() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            LiteratureSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 14L, 15L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: literature, DocumentOutput COUNT: 14, RDD COUNT: 15, Solr COUNT: 15",
                    error.getMessage());
        }
    }

    @Test
    void runValidationInvalidSolr() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            LiteratureSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 15L, 14L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: literature, DocumentOutput COUNT: 15, RDD COUNT: 15, Solr COUNT: 14",
                    error.getMessage());
        }
    }

    private LiteratureSolrIndexValidator prepareValidator(
            Config applicationConfig, JavaSparkContext context, long docCount, long solrCount)
            throws Exception {
        JobParameter jobParameter = getJobParameter(applicationConfig, context);
        return spyMockValidator(
                new LiteratureSolrIndexValidator(jobParameter), docCount, solrCount);
    }
}
