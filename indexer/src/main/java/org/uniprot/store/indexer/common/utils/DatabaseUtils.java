package org.uniprot.store.indexer.common.utils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseUtils {
    private PreparedStatement preparedStatement;
    private Connection dbConnection;

    public DatabaseUtils(DataSource dataSource, String query) throws SQLException {
        this.dbConnection = dataSource.getConnection();
        this.preparedStatement = this.dbConnection.prepareStatement(query);
    }

    public Pair<Long, Long> getProteinCount(String identifier) throws SQLException {
        this.preparedStatement.setString(1, identifier);
        Long revCount = 0L;
        Long unrevCount = 0L;
        try (ResultSet resultSet = this.preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                if (Long.valueOf(0).equals(resultSet.getLong(1))) { // 0 == reviewed, 1 == unreviewed
                    revCount = resultSet.getLong(2);
                } else if (Long.valueOf(1).equals(resultSet.getLong(1))) {
                    unrevCount = resultSet.getLong(2);
                }

            }
        }
        Pair<Long, Long> revUnrevCountPair = new ImmutablePair<>(revCount, unrevCount);

        return revUnrevCountPair;
    }
}
