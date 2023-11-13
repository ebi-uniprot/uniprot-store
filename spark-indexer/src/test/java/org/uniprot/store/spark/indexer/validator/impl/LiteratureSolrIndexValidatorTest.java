package org.uniprot.store.spark.indexer.validator.impl;

import com.typesafe.config.Config;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

class LiteratureSolrIndexValidatorTest {

    @Test
    void runValidationSuccess() throws Exception{
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try(JavaSparkContext context =
                    SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            JobParameter jobParameter = JobParameter.builder()
                    .releaseName("2020_02")
                    .applicationConfig(applicationConfig)
                    .sparkContext(context)
                    .build();

            LiteratureSolrIndexValidator validator = Mockito.spy(new LiteratureSolrIndexValidator(jobParameter));

            Mockito.doReturn(10L)
                    .when(validator)
                    .getOutputUniParcDocumentsCount();

            CloudSolrClient solrClient = Mockito.mock(CloudSolrClient.class);

            QueryResponse response = Mockito.mock(QueryResponse.class);
            SolrDocumentList queryResult = Mockito.mock(SolrDocumentList.class);
            Mockito.when(response.getResults())
                    .thenReturn(queryResult);
            Mockito.when(queryResult.getNumFound()).thenReturn(10L);

            Mockito.when(solrClient.query(Mockito.eq(validator.getCollection().name()), Mockito.any()))
                    .thenReturn(response);

            Mockito.doReturn(solrClient)
                    .when(validator)
                    .getSolrClient(Mockito.any());

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalid() throws Exception{
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try(JavaSparkContext context =
                    SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            JobParameter jobParameter = JobParameter.builder()
                    .releaseName("2020_02")
                    .applicationConfig(applicationConfig)
                    .sparkContext(context)
                    .build();

            LiteratureSolrIndexValidator validator = Mockito.spy(new LiteratureSolrIndexValidator(jobParameter));

            Mockito.doReturn(10L)
                    .when(validator)
                    .getOutputUniParcDocumentsCount();

            CloudSolrClient solrClient = Mockito.mock(CloudSolrClient.class);

            QueryResponse response = Mockito.mock(QueryResponse.class);
            SolrDocumentList queryResult = Mockito.mock(SolrDocumentList.class);
            Mockito.when(response.getResults())
                    .thenReturn(queryResult);
            Mockito.when(queryResult.getNumFound()).thenReturn(10L);

            Mockito.when(solrClient.query(Mockito.eq(validator.getCollection().name()), Mockito.any()))
                    .thenReturn(response);

            Mockito.doReturn(solrClient)
                    .when(validator)
                    .getSolrClient(Mockito.any());

            assertDoesNotThrow(validator::runValidation);
        }
    }
}