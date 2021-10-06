package org.uniprot.store.indexer.taxonomy.readers;

import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.Getter;

import org.springframework.jdbc.core.RowMapper;
import org.uniprot.core.taxonomy.impl.TaxonomyStrainBuilder;
import org.uniprot.core.util.Utils;

/** @author lgonzales */
public class TaxonomyStrainReader implements RowMapper<TaxonomyStrainReader.Strain> {

    @Override
    public Strain mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        long id = resultSet.getLong("strain_id");
        String name = resultSet.getString("name");
        StrainNameClass nameClass = StrainNameClass.fromQuery(resultSet.getString("name_class"));

        return new Strain(id, name, nameClass);
    }

    public enum StrainNameClass {
        scientific_name,
        synonym;

        static StrainNameClass fromQuery(String value) {
            if (Utils.notNullNotEmpty(value)) {
                if (value.equals("scientific name")) {
                    return StrainNameClass.scientific_name;
                } else if (value.equals("synonym")) {
                    return StrainNameClass.synonym;
                }
            }
            return null;
        }
    }

    @Getter
    public static class Strain {
        private final long id;
        private final String name;
        private final StrainNameClass nameClass;

        public Strain(long id, String name, StrainNameClass nameClass) {
            this.id = id;
            this.name = name;
            this.nameClass = nameClass;
        }
    }
}
