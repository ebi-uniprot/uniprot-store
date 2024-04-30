package org.uniprot.store.indexer.disease;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @author sahmad
 */
public class DiseaseProteinCountReader
        implements RowMapper<DiseaseProteinCountReader.DiseaseProteinCount> {

    @Override
    public DiseaseProteinCountReader.DiseaseProteinCount mapRow(ResultSet resultSet, int rowIndex)
            throws SQLException {
        String diseaseId = resultSet.getString("diseaseId");
        long proteinCount = resultSet.getLong("proteinCount");

        return new DiseaseProteinCount(diseaseId, proteinCount);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class DiseaseProteinCount {
        private final String diseaseId;
        private final long reviewedProteinCount;
    }
}
