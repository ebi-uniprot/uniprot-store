package org.uniprot.store.spark.indexer.taxonomy;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.*;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 24/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaxonomyRDDReaderTest {

    private Connection dbConnection;
    private JobParameter parameter;

    @BeforeAll
    public void setupTests() throws SQLException, IOException {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application);
        parameter =
                JobParameter.builder()
                        .applicationConfig(application)
                        .releaseName("2020_02")
                        .sparkContext(sparkContext)
                        .build();

        String url = application.getString("database.url");
        String user = application.getString("database.user.name");
        String password = application.getString("database.password");
        dbConnection = DriverManager.getConnection(url, user, password);
        fillDatabase();
    }

    @Test
    void load() {
        assertNotNull(parameter);
        assertNotNull(dbConnection);
    }

    @Test
    void loadWithLineage() {
        assertNotNull(parameter);
        assertNotNull(dbConnection);
    }

    @Test
    void getMaxTaxId() {
        assertNotNull(parameter);
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
}
