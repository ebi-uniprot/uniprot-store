package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.indexer.configure.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;
/**
 *
 * @author lgonzales
 */
public class TaxonomyNamesReader implements RowMapper<TaxonomyDocument> {

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int i) throws SQLException {
        return null;
    }
}
