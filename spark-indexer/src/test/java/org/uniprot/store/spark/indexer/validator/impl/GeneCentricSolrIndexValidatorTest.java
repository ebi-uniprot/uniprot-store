package org.uniprot.store.spark.indexer.validator.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

class GeneCentricSolrIndexValidatorTest {

    @Test
    void runValidationSuccess() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            GeneCentricSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 40L, 40L);

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalidDocument() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            GeneCentricSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 39L, 40L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: genecentric, DocumentOutput COUNT: 39, RDD COUNT: 40, Solr COUNT: 40",
                    error.getMessage());
        }
    }

    @Test
    void runValidationInvalidSolr() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            GeneCentricSolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 40L, 39L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: genecentric, DocumentOutput COUNT: 40, RDD COUNT: 40, Solr COUNT: 39",
                    error.getMessage());
        }
    }

    private GeneCentricSolrIndexValidator prepareValidator(
            Config applicationConfig, JavaSparkContext context, long docCount, long solrCount)
            throws Exception {
        JobParameter jobParameter =
                JobParameter.builder()
                        .releaseName("2020_02")
                        .applicationConfig(applicationConfig)
                        .sparkContext(context)
                        .build();

        GeneCentricSolrIndexValidator validator =
                Mockito.spy(new GeneCentricSolrIndexValidator(jobParameter));

        Mockito.doReturn(docCount).when(validator).getOutputDocumentsCount();

        CloudSolrClient solrClient = Mockito.mock(CloudSolrClient.class);

        QueryResponse response = Mockito.mock(QueryResponse.class);
        SolrDocumentList queryResult = Mockito.mock(SolrDocumentList.class);
        Mockito.when(response.getResults()).thenReturn(queryResult);
        Mockito.when(queryResult.getNumFound()).thenReturn(solrCount);

        Mockito.when(solrClient.query(Mockito.eq(validator.getCollection().name()), Mockito.any()))
                .thenReturn(response);

        Mockito.doReturn(solrClient).when(validator).getSolrClient(Mockito.any());
        return validator;
    }
}
