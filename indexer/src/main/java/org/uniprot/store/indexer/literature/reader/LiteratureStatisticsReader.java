package org.uniprot.store.indexer.literature.reader;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import lombok.Getter;

/**
 * @author lgonzales
 */
public class LiteratureStatisticsReader
        implements RowMapper<LiteratureStatisticsReader.LiteratureCount> {

    public static final String LITERATURE_STATISTICS_SQL =
            "SELECT ID as pubmed_id, REVIEWED_PROTEIN_COUNT as reviewedProteinCount, "
                    + "UNREVIEWED_PROTEIN_COUNT as unreviewedProteinCount "
                    + "FROM SPTR.MV_DATA_SOURCE_STATS WHERE DATA_TYPE = 'Literature'";

    @Override
    public LiteratureCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        long pubmedId = resultSet.getLong("pubmed_id");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        return new LiteratureCount(pubmedId, reviewedProteinCount, unreviewedProteinCount);
    }

    @Getter
    public static class LiteratureCount {
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
        private final long pubmedId;

        public LiteratureCount(
                long pubmedId, long reviewedProteinCount, long unreviewedProteinCount) {
            this.pubmedId = pubmedId;
            this.reviewedProteinCount = reviewedProteinCount;
            this.unreviewedProteinCount = unreviewedProteinCount;
        }
    }
}
