package org.uniprot.store.indexer.crossref.readers;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author sahmad
 */
@Slf4j
public class CrossRefUniProtCountReader implements RowMapper<CrossRefUniProtCountReader.CrossRefProteinCount> {

    @Override
    public CrossRefProteinCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String abbrev = resultSet.getString("abbrev");
        long proteinCount = resultSet.getLong("proteinCount");
        long entryType = resultSet.getLong("entryType");
        return new CrossRefProteinCount(abbrev, entryType, proteinCount);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class CrossRefProteinCount {
        private final String abbreviation;
        private final long entryType;
        private final long proteinCount;
    }
}
