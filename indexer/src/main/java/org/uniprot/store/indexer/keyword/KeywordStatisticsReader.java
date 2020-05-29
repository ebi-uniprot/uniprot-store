package org.uniprot.store.indexer.keyword;

import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.Getter;

import org.springframework.jdbc.core.RowMapper;

/** @author lgonzales */
public class KeywordStatisticsReader implements RowMapper<KeywordStatisticsReader.KeywordCount> {
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
