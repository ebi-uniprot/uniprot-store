package org.uniprot.store.indexer.taxonomy.readers;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import lombok.Getter;

/**
 * @author lgonzales
 */
public class TaxonomyStatisticsReader implements RowMapper<TaxonomyStatisticsReader.TaxonomyCount> {

    @Override
    public TaxonomyCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        long taxId = resultSet.getLong("tax_id");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        long referenceProteomeCount = resultSet.getLong("referenceProteomeCount");
        long otherProteomeCount = resultSet.getLong("proteomeCount");
        return new TaxonomyCount(
                taxId,
                reviewedProteinCount,
                unreviewedProteinCount,
                referenceProteomeCount,
                otherProteomeCount);
    }

    @Getter
    public static class TaxonomyCount {
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
        private final long referenceProteomeCount;
        private final long proteomeCount;
        private final long taxId;

        public TaxonomyCount(
                long taxId,
                long reviewedProteinCount,
                long unreviewedProteinCount,
                long referenceProteomeCount,
                long proteomeCount) {
            this.taxId = taxId;
            this.reviewedProteinCount = reviewedProteinCount;
            this.unreviewedProteinCount = unreviewedProteinCount;
            this.referenceProteomeCount = referenceProteomeCount;
            this.proteomeCount = proteomeCount;
        }
    }
}
