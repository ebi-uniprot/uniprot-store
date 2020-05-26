package org.uniprot.store.indexer.crossref.readers;

import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.jdbc.core.RowMapper;

/** @author sahmad */
@Slf4j
public class CrossRefUniProtCountReader
        implements RowMapper<CrossRefUniProtCountReader.CrossRefProteinCount> {

    public static final String QUERY_TO_GET_XREF_PROTEIN_COUNT =
            "SELECT ID as abbrev, REVIEWED_PROTEIN_COUNT as reviewedProteinCount, "
                    + "UNREVIEWED_PROTEIN_COUNT as unreviewedProteinCount "
                    + "FROM SPTR.MV_DATA_SOURCE_STATS WHERE DATA_TYPE = 'Cross Ref'";

    @Override
    public CrossRefProteinCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String abbrev = resultSet.getString("abbrev");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        return new CrossRefProteinCount(abbrev, reviewedProteinCount, unreviewedProteinCount);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class CrossRefProteinCount {
        private final String abbreviation;
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
    }
}
