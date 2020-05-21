package org.uniprot.store.indexer.keyword;

import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.Getter;

import org.springframework.jdbc.core.RowMapper;

/** @author lgonzales */
public class KeywordStatisticsReader implements RowMapper<KeywordStatisticsReader.KeywordCount> {
    public static final String KEYWORD_STATISTICS_URL =
            "SELECT ID as accession, REVIEWED_PROTEIN_COUNT as reviewedProteinCount, "
                    + "UNREVIEWED_PROTEIN_COUNT as unreviewedProteinCount "
                    + "FROM SPTR.MV_DATA_SOURCE_STATS WHERE DATA_TYPE = 'Keyword'";

    @Override
    public KeywordCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String keywordId = resultSet.getString("accession");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        return new KeywordCount(keywordId, reviewedProteinCount, unreviewedProteinCount);
    }

    @Getter
    public static class KeywordCount {
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
        private final String keywordId;

        public KeywordCount(
                String keywordId, long reviewedProteinCount, long unreviewedProteinCount) {
            this.keywordId = keywordId;
            this.reviewedProteinCount = reviewedProteinCount;
            this.unreviewedProteinCount = unreviewedProteinCount;
        }
    }
}
