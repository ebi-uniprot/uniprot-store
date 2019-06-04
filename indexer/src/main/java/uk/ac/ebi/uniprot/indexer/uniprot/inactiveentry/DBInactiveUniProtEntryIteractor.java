package uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class DBInactiveUniProtEntryIteractor extends AbstractInactiveUniProtEntryIterator implements AutoCloseable {
    private final DataSource dataSource;
    private ResultSet resultSet;
    private Statement stmt;
    private Connection connection;
    private static final String SQL = "select accession_subject, entry_name, operation, accession_object "
            + "from DELETED_MERGED_ACCESSION order by accession_subject";

    public DBInactiveUniProtEntryIteractor(DataSource dataSource) {
        this.dataSource = dataSource;
        init();
    }

    private void init() {
        try {
            connection = dataSource.getConnection();
            stmt = connection.createStatement();
            resultSet = stmt.executeQuery(SQL);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected InactiveUniProtEntry nextEntry() {
        try {
            hasResult = resultSet.next();
            if (hasResult) {
                String accession = resultSet.getString(1);
                String id = resultSet.getString(2);
                String reason = resultSet.getString(3);
                String objAccession = resultSet.getString(4);
                return InactiveUniProtEntry.from(accession, id, reason, objAccession);
            } else
                return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        resultSet.close();
        stmt.close();
        connection.close();

    }

}
