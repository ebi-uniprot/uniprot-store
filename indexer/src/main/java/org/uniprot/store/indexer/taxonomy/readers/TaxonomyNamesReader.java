package org.uniprot.store.indexer.taxonomy.readers;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

/** @author lgonzales */
public class TaxonomyNamesReader implements RowMapper<String> {

    @Override
    public String mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        return resultSet.getString("NAME");
    }
}
