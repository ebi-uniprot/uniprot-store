package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/**
 *
 * @author lgonzales
 */
public class TaxonomyStrainReader implements RowMapper<TaxonomyDocument> {

    @Override
    public TaxonomyDocument mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyDocument.TaxonomyDocumentBuilder builder = TaxonomyDocument.builder();
        builder.id(""+resultSet.getLong("TAX_ID"));
        builder.taxId(resultSet.getLong("TAX_ID"));
        String strainName = "";
        String scientificName =resultSet.getString("scientific_name");
        if(scientificName != null){
            strainName += scientificName;
        }
        String synonymName =resultSet.getString("synonym_name");
        if(synonymName != null){
            strainName += ", "+synonymName;
        }
        builder.strain(Collections.singletonList(strainName));
        return builder.build();
    }
}