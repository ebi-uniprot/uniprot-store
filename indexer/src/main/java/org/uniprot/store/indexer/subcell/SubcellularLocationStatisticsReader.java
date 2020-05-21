package org.uniprot.store.indexer.subcell;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.jdbc.core.RowMapper;

/**
 * @author lgonzales
 * @since 2019-07-12
 */
@Slf4j
public class SubcellularLocationStatisticsReader
        implements RowMapper<SubcellularLocationStatisticsReader.SubcellularLocationCount> {

    public static final String SUBCELLULAR_LOCATION_STATISTICS_QUERY =
            "SELECT ID as identifier, REVIEWED_PROTEIN_COUNT as reviewedProteinCount, "
                    + "UNREVIEWED_PROTEIN_COUNT as unreviewedProteinCount "
                    + "FROM SPTR.MV_DATA_SOURCE_STATS WHERE DATA_TYPE = 'Subcellular Location'";

    @Override
    public SubcellularLocationCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String subcellularLocationId = resultSet.getString("identifier");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        return new SubcellularLocationCount(
                subcellularLocationId, reviewedProteinCount, unreviewedProteinCount);
    }

    @Getter
    public static class SubcellularLocationCount implements Serializable {

        private static final long serialVersionUID = 542822089533820233L;
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
        private final String subcellularLocationId;

        public SubcellularLocationCount(
                String subcellularLocationId,
                long reviewedProteinCount,
                long unreviewedProteinCount) {
            this.subcellularLocationId = subcellularLocationId;
            this.reviewedProteinCount = reviewedProteinCount;
            this.unreviewedProteinCount = unreviewedProteinCount;
        }
    }
}
