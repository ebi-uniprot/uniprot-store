package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.common.Utils;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

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
        Long taxId = resultSet.getLong("TAX_ID");
        builder.taxId(taxId);
        builder.id(String.valueOf(taxId));

        String strainName = "";
        String scientificName =resultSet.getString("scientific_name");
        if(Utils.notEmpty(scientificName)){
            strainName += scientificName;
        }
        String synonymName =resultSet.getString("synonym_name");
        if(Utils.notEmpty(synonymName)){
            strainName += ", "+synonymName;
        }
        builder.strain(Collections.singletonList(strainName));
        return builder.build();
    }
}