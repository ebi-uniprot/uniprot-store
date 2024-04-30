package org.uniprot.store.indexer.arba;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
@Slf4j
public class ArbaProteinCountReader implements RowMapper<ArbaProteinCountReader.ArbaProteinCount> {

    public static final String ARBA_PROTEIN_COUNT_QUERY =
            "SELECT ID AS ruleId, UNREVIEWED_PROTEIN_COUNT AS proteinCount "
                    + " FROM SPTR.MV_DATA_SOURCE_STATS WHERE DATA_TYPE = 'ARBA'";

    @Override
    public ArbaProteinCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String ruleId = resultSet.getString("ruleId");
        long proteinCount = resultSet.getLong("proteinCount");
        return new ArbaProteinCount(ruleId, proteinCount);
    }

    @Getter
    @AllArgsConstructor
    public static class ArbaProteinCount implements Serializable {
        private final String ruleId;
        private final long proteinCount;
    }
}
