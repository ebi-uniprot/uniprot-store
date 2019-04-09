package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;

import uk.ac.ebi.uniprot.indexer.document.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/**
 *
 * @author lgonzales
 */
public class TaxonomyURLReader implements RowMapper<TaxonomyDocument> {

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyDocument.TaxonomyDocumentBuilder builder = TaxonomyDocument.builder();
        Long taxId = resultSet.getLong("TAX_ID");
        builder.id(String.valueOf(taxId));
        builder.taxId(taxId);

        String url = resultSet.getString("URI");
        builder.url(Collections.singletonList(url));
        return builder.build();
    }
}
