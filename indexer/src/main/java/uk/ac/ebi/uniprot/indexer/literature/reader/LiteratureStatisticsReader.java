package uk.ac.ebi.uniprot.indexer.literature.reader;

import lombok.Getter;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author lgonzales
 */
public class LiteratureStatisticsReader implements RowMapper<LiteratureStatisticsReader.LiteratureCount> {

    @Override
    public LiteratureCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String pubmedId = resultSet.getString("pubmed_id");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        return new LiteratureCount(pubmedId, reviewedProteinCount, unreviewedProteinCount);
    }

    @Getter
    public static class LiteratureCount {
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
        private final String pubmedId;


        public LiteratureCount(String pubmedId, long reviewedProteinCount, long unreviewedProteinCount) {
            this.pubmedId = pubmedId;
            this.reviewedProteinCount = reviewedProteinCount;
            this.unreviewedProteinCount = unreviewedProteinCount;
        }
    }
}
