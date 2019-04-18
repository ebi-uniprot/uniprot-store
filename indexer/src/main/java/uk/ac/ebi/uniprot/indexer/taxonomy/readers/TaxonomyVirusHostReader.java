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
public class TaxonomyVirusHostReader implements RowMapper<TaxonomyDocument> {

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyDocument.TaxonomyDocumentBuilder builder = TaxonomyDocument.builder();
        Long virusHost =resultSet.getLong("HOST_ID");
        builder.host(Collections.singletonList(virusHost));

        Long taxId = resultSet.getLong("TAX_ID");
        builder.id(String.valueOf(taxId));
        builder.taxId(taxId);
        return builder.build();
    }
}
