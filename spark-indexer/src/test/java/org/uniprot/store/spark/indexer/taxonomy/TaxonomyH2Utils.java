package org.uniprot.store.spark.indexer.taxonomy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author lgonzales
 * @since 25/11/2020
 */
public class TaxonomyH2Utils {

    public static void createTables(Statement statement) throws IOException, SQLException {
        String getCreateTablesSql =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        "src/test/resources/2020_02/taxonomy/create_tables.sql")));
        statement.execute(getCreateTablesSql);
    }

    public static void insertData(Statement statement) throws SQLException, IOException {
        String getInsertDataSql =
                new String(
                        Files.readAllBytes(
                                Paths.get("src/test/resources/2020_02/taxonomy/insert_data.sql")));
        statement.execute(getInsertDataSql);
    }

    public static void dropTables(Statement statement) throws IOException, SQLException {
        String getDropTablesSql =
                new String(
                        Files.readAllBytes(
                                Paths.get("src/test/resources/2020_02/taxonomy/drop_tables.sql")));
        statement.execute(getDropTablesSql);
    }
}
