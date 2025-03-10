package org.uniprot.store.spark.indexer.taxonomy.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.TaxDb.READ;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 24/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaxonomyRDDReaderTest {

    private Connection dbConnection;
    private Config application;

    @BeforeAll
    public void setupTests() throws SQLException, IOException {
        application = SparkUtils.loadApplicationProperty();
        String url = application.getString("database.read.url");
        String user = application.getString("database.read.user.name");
        String password = application.getString("database.read.password");
        dbConnection = DriverManager.getConnection(url, user, password);
        fillDatabase();
    }

    @Test
    void load() {
        assertNotNull(dbConnection);
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .taxDb(READ)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            TaxonomyRDDReader reader = new TaxonomyRDDReader(parameter, false);
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD = reader.load();
            assertNotNull(taxonomyRDD);
            long count = taxonomyRDD.count();
            assertEquals(11, count);

            Tuple2<String, TaxonomyEntry> tuple = taxonomyRDD.first();
            assertNotNull(tuple);
            assertNotNull(tuple._1);
            assertNotNull(tuple._2);
            assertEquals("1", tuple._1);
            TaxonomyEntry entry = tuple._2;
            assertEquals(1L, entry.getTaxonId());
            assertEquals("9ZZZZ", entry.getMnemonic());
        }
    }

    @Test
    void loadWithLineage() {
        assertNotNull(dbConnection);
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .taxDb(READ)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            TaxonomyRDDReaderFake reader = new TaxonomyRDDReaderFake(parameter, true, false);
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD = reader.load();
            assertNotNull(taxonomyRDD);
            long count = taxonomyRDD.count();
            assertEquals(10, count);

            List<Tuple2<String, TaxonomyEntry>> tuples = taxonomyRDD.take(10);
            TaxonomyEntry taxWithLineage =
                    tuples.stream()
                            .map(Tuple2::_2)
                            .filter(entry -> entry.getTaxonId() == 10116L)
                            .findFirst()
                            .orElseThrow(AssertionError::new);

            assertNotNull(taxWithLineage);
            assertEquals(10116L, taxWithLineage.getTaxonId());
            assertNotNull(taxWithLineage.getLineages());
            assertEquals(3, taxWithLineage.getLineages().size());
            assertEquals(10114L, taxWithLineage.getLineages().get(2).getTaxonId());
            assertEquals(39107L, taxWithLineage.getLineages().get(1).getTaxonId());
            assertEquals(10066L, taxWithLineage.getLineages().get(0).getTaxonId());
        }
    }

    @Test
    void getMaxTaxId() {
        assertNotNull(dbConnection);
    }

    @AfterAll
    public void teardown() throws SQLException, IOException {
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.dropTables(statement);
        dbConnection.close();
    }

    private void fillDatabase() throws SQLException, IOException {
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.createTables(statement);
        TaxonomyH2Utils.insertData(statement);
    }
}
