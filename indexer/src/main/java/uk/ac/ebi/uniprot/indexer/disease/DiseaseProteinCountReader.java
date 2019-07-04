package uk.ac.ebi.uniprot.indexer.disease;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author sahmad
 */
public class DiseaseProteinCountReader implements RowMapper<DiseaseProteinCountReader.DiseaseProteinCount> {

    @Override
    public DiseaseProteinCountReader.DiseaseProteinCount mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        String diseaseId = resultSet.getString("diseaseId");
        long entryType = resultSet.getLong("entryType");
        long proteinCount = resultSet.getLong("proteinCount");
        long reviewedCount = 0;
        long unreviewedCount = 0;
        if (Long.valueOf(0).equals(entryType)) { // 0 == reviewed,
            reviewedCount = proteinCount;
        } else {// 1 == unreviewed
            unreviewedCount = proteinCount;
        }

        return new DiseaseProteinCount(diseaseId, reviewedCount, unreviewedCount);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class DiseaseProteinCount {
        private final String diseaseId;
        private final long reviewedProteinCount;
        private final long unreviewedProteinCount;
    }
}
