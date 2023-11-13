package org.uniprot.store.spark.indexer.validator.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyH2Utils;

import com.typesafe.config.Config;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaxonomySolrIndexValidatorTest {

    private Connection dbConnection;

    @BeforeAll
    void setUpDatabase() throws SQLException, IOException {
        Config application = SparkUtils.loadApplicationProperty("application-taxonomy");

        // Taxonomy H2 database create/load database data
        String url = application.getString("database.url");
        String user = application.getString("database.user.name");
        String password = application.getString("database.password");
        dbConnection = DriverManager.getConnection(url, user, password);
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.createTables(statement);
        TaxonomyH2Utils.insertData(statement);
    }

    @AfterAll
    void closeDatabase() throws SQLException, IOException {
        // Taxonomy H2 database clean
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.dropTables(statement);
        dbConnection.close();
    }

    @Test
    void runValidationSuccess() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            TaxonomySolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 15L, 15L);

            assertDoesNotThrow(validator::runValidation);
        }
    }

    @Test
    void runValidationInvalidDocument() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            TaxonomySolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 14L, 15L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: taxonomy, DocumentOutput COUNT: 14, RDD COUNT: 15, Solr COUNT: 15",
                    error.getMessage());
        }
    }

    @Test
    void runValidationInvalidSolr() throws Exception {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext context =
                SparkUtils.loadSparkContext(applicationConfig, SPARK_LOCAL_MASTER)) {

            TaxonomySolrIndexValidator validator =
                    prepareValidator(applicationConfig, context, 15L, 14L);

            SparkIndexException error =
                    assertThrows(SparkIndexException.class, validator::runValidation);
            assertEquals(
                    "Total Entries does not match. Collection: taxonomy, DocumentOutput COUNT: 15, RDD COUNT: 15, Solr COUNT: 14",
                    error.getMessage());
        }
    }

    private TaxonomySolrIndexValidator prepareValidator(
            Config applicationConfig, JavaSparkContext context, long docCount, long solrCount)
            throws Exception {
        JobParameter jobParameter =
                JobParameter.builder()
                        .releaseName("2020_02")
                        .applicationConfig(applicationConfig)
                        .sparkContext(context)
                        .build();

        TaxonomySolrIndexValidator validator =
                Mockito.spy(new TaxonomySolrIndexValidator(jobParameter));

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
