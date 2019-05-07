package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.common.Utils;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyRank;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyEntryBuilder;
import uk.ac.ebi.uniprot.indexer.taxonomy.steps.TaxonomyNodeStep;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class is mapping {@link ResultSet} returned by SQL executed in
 *  {@link TaxonomyNodeStep}
 *  to {@link TaxonomyDocument} object that will be used to save Taxonomy Nodes
 *
 *  @author lgonzales
 */
public class TaxonomyNodeReader implements RowMapper<TaxonomyEntryBuilder> {

    @Override
    public TaxonomyEntryBuilder mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder();
        builder.taxonId(resultSet.getLong("TAX_ID"));
        String common = resultSet.getString("SPTR_COMMON");
        if(common == null){
            common = resultSet.getString("NCBI_COMMON");
        }
        builder.commonName(common);
        String scientificName = resultSet.getString("SPTR_SCIENTIFIC");
        if(scientificName == null){
            scientificName = resultSet.getString("NCBI_SCIENTIFIC");
        }
        builder.scientificName(scientificName);

        builder.mnemonic(resultSet.getString("TAX_CODE"));
        builder.parentId(resultSet.getLong("PARENT_ID"));
        String rank = resultSet.getString("RANK");
        if(Utils.notEmpty(rank)) {
            try {
                builder.rank(TaxonomyRank.valueOf(rank));
            }catch (IllegalArgumentException iae){
                builder.rank(TaxonomyRank.NO_RANK);
            }
        }
        builder.addSynonyms(resultSet.getString("SPTR_SYNONYM"));
        builder.hidden(resultSet.getBoolean("HIDDEN"));
        builder.active(true);
        return builder;
    }
}
