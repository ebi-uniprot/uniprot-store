package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import lombok.Getter;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
public class TaxonomyStatisticsReader implements RowMapper<TaxonomyStatisticsReader.TaxonomyCount> {

    @Override
    public TaxonomyCount mapRow(ResultSet resultSet, int i) throws SQLException {
        long taxId =resultSet.getLong("tax_id");
        long reviewedProteinCount = resultSet.getLong("reviewedProteinCount");
        long unreviewedProteinCount = resultSet.getLong("unreviewedProteinCount");
        long proteomeCount = resultSet.getLong("proteomeCount");
        return new TaxonomyCount(taxId, reviewedProteinCount,unreviewedProteinCount,proteomeCount);
    }

    @Getter
    public static class TaxonomyCount {
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
        private final long proteomeCount;
        private final long taxId;


        public TaxonomyCount(long taxId, long reviewedProteinCount,long unreviewedProteinCount,long proteomeCount){
            this.taxId = taxId;
            this.reviewedProteinCount = reviewedProteinCount;
            this.unreviewedProteinCount = unreviewedProteinCount;
            this.proteomeCount = proteomeCount;
        }
    }

}
