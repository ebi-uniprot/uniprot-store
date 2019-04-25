package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/**
 *
 * @author lgonzales
 */
public class TaxonomyNamesReader implements RowMapper<TaxonomyDocument> {

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyDocument.TaxonomyDocumentBuilder builder = TaxonomyDocument.builder();
        builder.id(""+resultSet.getLong("TAX_ID"));
        String name = resultSet.getString("NAME");
        builder.otherNames(Collections.singletonList(name));

        return builder.build();
    }
}
