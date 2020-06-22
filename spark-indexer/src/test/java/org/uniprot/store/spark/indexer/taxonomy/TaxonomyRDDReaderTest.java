package org.uniprot.store.spark.indexer.taxonomy;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.*;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 24/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaxonomyRDDReaderTest {

    private Connection dbConnection;
    private ResourceBundle application;

    @BeforeAll
    public void setupTests() throws SQLException, IOException {
        application = SparkUtils.loadApplicationProperty();
        String url = application.getString("database.url");
        String user = application.getString("database.user.name");
        String password = application.getString("database.password");
        dbConnection = DriverManager.getConnection(url, user, password);
        fillDatabase();
    }

    @Test
    void load() {
        assertNotNull(dbConnection);
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            TaxonomyRDDReader reader = new TaxonomyRDDReader(parameter, false);
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD = reader.load();
            assertNotNull(taxonomyRDD);
            long count = taxonomyRDD.count();
            assertEquals(6, count);

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
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            TaxonomyRDDReaderFake reader = new TaxonomyRDDReaderFake(parameter, true);
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD = reader.load();
            assertNotNull(taxonomyRDD);
            long count = taxonomyRDD.count();
            assertEquals(2, count);

            List<Tuple2<String, TaxonomyEntry>> tuples = taxonomyRDD.take(2);
            TaxonomyEntry taxWithLineage =
                    tuples.stream()
                            .map(Tuple2::_2)
                            .filter(entry -> entry.getTaxonId() == 337687L)
                            .findFirst()
                            .orElseThrow(AssertionError::new);

            assertNotNull(taxWithLineage);
            assertEquals(337687L, taxWithLineage.getTaxonId());
            assertNotNull(taxWithLineage.getLineages());
            assertEquals(2, taxWithLineage.getLineages().size());
            assertEquals(100L, taxWithLineage.getLineages().get(0).getTaxonId());
        }
    }

    @Test
    void getMaxTaxId() {
        assertNotNull(dbConnection);
    }

    @AfterAll
    public void teardown() throws SQLException, IOException {
        Statement statement = this.dbConnection.createStatement();
        dropTables(statement);
        dbConnection.close();
    }

    private void fillDatabase() throws SQLException, IOException {
        Statement statement = this.dbConnection.createStatement();
        createTables(statement);
        insertData(statement);
    }

    private void createTables(Statement statement) throws IOException, SQLException {
        String getCreateTablesSql =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        "src/test/resources/2020_02/taxonomy/create_tables.sql")));
        statement.execute(getCreateTablesSql);
    }

    private void insertData(Statement statement) throws SQLException, IOException {
        String getInsertDataSql =
                new String(
                        Files.readAllBytes(
                                Paths.get("src/test/resources/2020_02/taxonomy/insert_data.sql")));
        statement.execute(getInsertDataSql);
    }

    private void dropTables(Statement statement) throws IOException, SQLException {
        String getDropTablesSql =
                new String(
                        Files.readAllBytes(
                                Paths.get("src/test/resources/2020_02/taxonomy/drop_tables.sql")));
        statement.execute(getDropTablesSql);
    }

    private static class TaxonomyRDDReaderFake extends TaxonomyRDDReader {

        private final JobParameter jobParameter;

        public TaxonomyRDDReaderFake(JobParameter jobParameter, boolean withLineage) {
            super(jobParameter, withLineage);
            this.jobParameter = jobParameter;
        }

        @Override
        protected JavaPairRDD<String, List<TaxonomyLineage>> loadTaxonomyLineage() {
            List<Tuple2<String, List<TaxonomyLineage>>> lineage = new ArrayList<>();
            List<TaxonomyLineage> lineages = new ArrayList<>();
            lineages.add(new TaxonomyLineageBuilder().taxonId(100).build());
            lineages.add(new TaxonomyLineageBuilder().taxonId(200).build());
            lineage.add(new Tuple2<>("337687", lineages));

            lineages = new ArrayList<>();
            lineages.add(new TaxonomyLineageBuilder().taxonId(300).build());
            lineages.add(new TaxonomyLineageBuilder().taxonId(400).build());
            lineage.add(new Tuple2<>("39107", lineages));

            return jobParameter.getSparkContext().parallelizePairs(lineage);
        }
    }
}
