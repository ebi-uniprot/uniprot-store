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

class PublicationSolrIndexValidatorTest {

    @Test
    void runValidationSuccess() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            PublicationSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 68L, 68L);

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalidDocument() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            PublicationSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 67L, 68L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: publication, DocumentOutput COUNT: 67, RDD COUNT: 68, Solr COUNT: 68",
                    error.getMessage());
        }
    }

    @Test
    void runValidationInvalidSolr() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            PublicationSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 68L, 67L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: publication, DocumentOutput COUNT: 68, RDD COUNT: 68, Solr COUNT: 67",
                    error.getMessage());
        }
    }

    private PublicationSolrIndexValidator prepareValidator(
            Config applicationConfig, JavaSparkContext context, long docCount, long solrCount)
            throws Exception {
        JobParameter jobParameter = getJobParameter(applicationConfig, context);
        return spyMockValidator(
                new PublicationSolrIndexValidator(jobParameter), docCount, solrCount);
    }
}
