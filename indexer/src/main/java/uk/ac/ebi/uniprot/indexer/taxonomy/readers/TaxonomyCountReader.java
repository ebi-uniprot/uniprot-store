package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
public class TaxonomyCountReader implements RowMapper<TaxonomyDocument> {

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyDocument.TaxonomyDocumentBuilder builder = TaxonomyDocument.builder();

        long count = resultSet.getLong("protein_count");
        int entryType =resultSet.getInt("entry_type");

        if(entryType == 0){
            builder.swissprotCount(count);
        }else if(entryType == 1){
            builder.tremblCount(count);
        }

        Long taxId = resultSet.getLong("TAX_ID");
        builder.id(String.valueOf(taxId));
        builder.taxId(taxId);
        return builder.build();
    }
}
