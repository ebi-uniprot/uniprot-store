package org.uniprot.store.indexer.arba;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.jdbc.core.RowMapper;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
@Slf4j
public class ArbaProteinCountReader implements RowMapper<ArbaProteinCountReader.ArbaProteinCount> {

    public static final String UNIRULE_PROTEIN_COUNT_QUERY =
            "SELECT ID AS oldRuleId, REVIEWED_PROTEIN_COUNT AS reviewedProteinCount, "
                    + "UNREVIEWED_PROTEIN_COUNT AS unreviewedProteinCount FROM SPTR.MV_DATA_SOURCE_STATS  WHERE  DATA_TYPE = 'UniRule'";

    @Override
    public ArbaProteinCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String oldRuleId = resultSet.getString("oldRuleId");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        return new ArbaProteinCount(oldRuleId, reviewedProteinCount, unreviewedProteinCount);
    }

    @Getter
    @AllArgsConstructor
    public static class ArbaProteinCount implements Serializable {
        private final String oldRuleId;
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
    }
}
