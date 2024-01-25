package org.uniprot.store.spark.indexer.validator.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;
import static org.uniprot.store.spark.indexer.validator.impl.SolrIndexValidatorUtil.getJobParameter;
import static org.uniprot.store.spark.indexer.validator.impl.SolrIndexValidatorUtil.wrapMockSolrIndexValidator;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

class UniRefSolrIndexValidatorTest {

    @Test
    void runValidationSuccess() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            UniRefSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 3L, 3L);

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalidDocument() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            UniRefSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 2L, 3L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: uniref, DocumentOutput COUNT: 2, RDD COUNT: 3, Solr COUNT: 3",
                    error.getMessage());
        }
    }

    @Test
    void runValidationInvalidSolr() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            UniRefSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 3L, 2L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: uniref, DocumentOutput COUNT: 3, RDD COUNT: 3, Solr COUNT: 2",
                    error.getMessage());
        }
    }

    private UniRefSolrIndexValidator prepareValidator(
            Config applicationConfig, JavaSparkContext context, long docCount, long solrCount)
            throws Exception {
        JobParameter jobParameter = getJobParameter(applicationConfig, context);
        return wrapMockSolrIndexValidator(
                new UniRefSolrIndexValidator(jobParameter), docCount, solrCount);
    }
}
