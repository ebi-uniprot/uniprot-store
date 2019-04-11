package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;

import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
public class TaxonomyCountReader implements RowMapper<TaxonomyDocument> {

    private static final int SWISS_PROT = 0;
    private static final int TREMBL = 1;

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyDocument.TaxonomyDocumentBuilder builder = TaxonomyDocument.builder();

        long count = resultSet.getLong("protein_count");
        int entryType =resultSet.getInt("entry_type");

        if(entryType == SWISS_PROT){
            builder.swissprotCount(count);
        }else if(entryType == TREMBL){
            builder.tremblCount(count);
        }

        Long taxId = resultSet.getLong("TAX_ID");
        builder.id(String.valueOf(taxId));
        builder.taxId(taxId);
        return builder.build();
    }
}
