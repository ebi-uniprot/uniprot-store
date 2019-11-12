package org.uniprot.store.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.core.util.Utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lgonzales
 */
public class TaxonomyLineageReader implements RowMapper<List<TaxonomyLineage>> {

    @Override
    public List<TaxonomyLineage> mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        List<TaxonomyLineage> lineageList = new ArrayList<>();
        String lineageIds = resultSet.getString("lineage_id");
        String lineageName = resultSet.getString("lineage_name");
        String lineageRank = resultSet.getString("lineage_rank");
        String lineageHidden = resultSet.getString("lineage_hidden");
        if (Utils.notNullOrEmpty(lineageIds)) {
            String[] lineageIdArray = lineageIds.substring(1).split("\\|");
            String[] lineageNameArray = lineageName.substring(1).split("\\|");
            String[] lineageRankArray = lineageRank.substring(1).split("\\|");
            String[] lineageHiddenArray = lineageHidden.substring(1).split("\\|");
            for (int i = 1; i < lineageIdArray.length - 1; i++) {
                TaxonomyLineageBuilder builder = new TaxonomyLineageBuilder();
                builder.taxonId(Long.parseLong(lineageIdArray[i]));
                builder.scientificName(lineageNameArray[i]);
                if (Utils.notNullOrEmpty(lineageRankArray[i])) {
                    try {
                        builder.rank(TaxonomyRank.valueOf(lineageRankArray[i].toUpperCase()));
                    } catch (IllegalArgumentException iae) {
                        builder.rank(TaxonomyRank.NO_RANK);
                    }
                }
                builder.hidden(lineageHiddenArray[i].equals("1"));
                lineageList.add(builder.build());
            }
        }
        return lineageList;
    }

}
