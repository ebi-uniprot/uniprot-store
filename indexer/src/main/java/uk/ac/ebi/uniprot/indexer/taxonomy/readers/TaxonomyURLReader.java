package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author lgonzales
 */
public class TaxonomyURLReader implements RowMapper<String> {

    @Override
    public String mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        return resultSet.getString("URI");
    }
}
