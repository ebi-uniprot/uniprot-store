package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.common.Utils;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyLineage;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyRank;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyLineageBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
/**
 *
 * @author lgonzales
 */
public class TaxonomyLineageReader implements RowMapper<List<TaxonomyLineage>> {

    @Override
    public List<TaxonomyLineage> mapRow(ResultSet resultSet, int index) throws SQLException {
        List<TaxonomyLineage> lineageList = new ArrayList<>();
        String lineageIds = resultSet.getString("lineage_id");
        String lineageName = resultSet.getString("lineage_name");
        String lineageRank = resultSet.getString("lineage_rank");
        String lineageHidden = resultSet.getString("lineage_hidden");
        if(Utils.notEmpty(lineageIds)) {
            String[] lineageIdArray = lineageIds.substring(1).split("\\|");
            String[] lineageNameArray = lineageName.substring(1).split("\\|");
            String[] lineageRankArray = lineageRank.substring(1).split("\\|");
            String[] lineageHiddenArray = lineageHidden.substring(1).split("\\|");
            String taxId = lineageIdArray[0];
            for(int i=1;i<lineageIdArray.length-1;i++){
                TaxonomyLineageBuilder builder = new TaxonomyLineageBuilder();
                builder.taxonId(Long.parseLong(lineageIdArray[i]));
                builder.scientificName(lineageNameArray[i]);
                builder.rank(TaxonomyRank.valueOf(lineageRankArray[i]));
                builder.hidden(lineageHiddenArray[i].equals("1"));
                lineageList.add(builder.build());
            }
        }
        return lineageList;
    }

}
