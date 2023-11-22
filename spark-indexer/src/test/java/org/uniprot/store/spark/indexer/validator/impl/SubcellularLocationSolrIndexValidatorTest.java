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

class SubcellularLocationSolrIndexValidatorTest {

    @Test
    void runValidationSuccess() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            SubcellularLocationSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 520L, 520L);

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalidDocument() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            SubcellularLocationSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 519L, 520L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: subcellularlocation, DocumentOutput COUNT: 519, RDD COUNT: 520, Solr COUNT: 520",
                    error.getMessage());
        }
    }

    @Test
    void runValidationInvalidSolr() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            SubcellularLocationSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 520L, 519L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: subcellularlocation, DocumentOutput COUNT: 520, RDD COUNT: 520, Solr COUNT: 519",
                    error.getMessage());
        }
    }

    private SubcellularLocationSolrIndexValidator prepareValidator(
            Config applicationConfig, JavaSparkContext context, long docCount, long solrCount)
            throws Exception {
        JobParameter jobParameter = getJobParameter(applicationConfig, context);
        return wrapMockSolrIndexValidator(
                new SubcellularLocationSolrIndexValidator(jobParameter), docCount, solrCount);
    }
}
