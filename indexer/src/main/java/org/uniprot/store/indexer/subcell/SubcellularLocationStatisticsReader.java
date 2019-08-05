package org.uniprot.store.indexer.subcell;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.RowMapper;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author lgonzales
 * @since 2019-07-12
 */
@Slf4j
public class SubcellularLocationStatisticsReader implements RowMapper<SubcellularLocationStatisticsReader.SubcellularLocationCount> {

    @Override
    public SubcellularLocationCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String subcellularLocationId = resultSet.getString("identifier");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        return new SubcellularLocationCount(subcellularLocationId, reviewedProteinCount, unreviewedProteinCount);
    }

    @Getter
    public static class SubcellularLocationCount implements Serializable {

        private static final long serialVersionUID = 542822089533820233L;
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
        private final String subcellularLocationId;


        public SubcellularLocationCount(String subcellularLocationId, long reviewedProteinCount, long unreviewedProteinCount) {
            this.subcellularLocationId = subcellularLocationId;
            this.reviewedProteinCount = reviewedProteinCount;
            this.unreviewedProteinCount = unreviewedProteinCount;
        }
    }
}
