package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.common.Utils;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TaxonomyLineageReader implements RowMapper<TaxonomyDocument> {

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int index) throws SQLException {
        TaxonomyDocument.TaxonomyDocumentBuilder builder = TaxonomyDocument.builder();
        String lineageIds = resultSet.getString("lineage_id");
        if(Utils.notEmpty(lineageIds)) {
            String[] lineageArray = lineageIds.substring(1).split("\\|");
            String taxId = lineageArray[0];
            List<Long> lineageList = new ArrayList<>();
            for(int i=1;i<lineageArray.length-1;i++){
                lineageList.add(Long.parseLong(lineageArray[i]));
            }
            builder.id(taxId);
            builder.lineage(lineageList);
        }
        return builder.build();
    }

}
