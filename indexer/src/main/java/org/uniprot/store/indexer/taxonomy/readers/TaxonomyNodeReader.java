package org.uniprot.store.indexer.taxonomy.readers;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.taxonomy.steps.TaxonomyNodeStep;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

/**
 * This class is mapping {@link ResultSet} returned by SQL executed in {@link TaxonomyNodeStep} to
 * {@link TaxonomyDocument} object that will be used to save Taxonomy Nodes
 *
 * @author lgonzales
 */
public class TaxonomyNodeReader implements RowMapper<TaxonomyEntry> {

    @Override
    public TaxonomyEntry mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
        TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder();
        builder.taxonId(resultSet.getLong("TAX_ID"));
        String common = resultSet.getString("SPTR_COMMON");
        if (common == null) {
            common = resultSet.getString("NCBI_COMMON");
        }
        builder.commonName(common);
        String scientificName = resultSet.getString("SPTR_SCIENTIFIC");
        if (scientificName == null) {
            scientificName = resultSet.getString("NCBI_SCIENTIFIC");
        }
        builder.scientificName(scientificName);

        builder.mnemonic(resultSet.getString("TAX_CODE"));
        builder.parentId(resultSet.getLong("PARENT_ID"));
        String rank = resultSet.getString("RANK");
        if (Utils.notNullNotEmpty(rank)) {
            try {
                builder.rank(TaxonomyRank.valueOf(rank.toUpperCase()));
            } catch (IllegalArgumentException iae) {
                builder.rank(TaxonomyRank.NO_RANK);
            }
        }
        builder.synonymsAdd(resultSet.getString("SPTR_SYNONYM"));
        builder.hidden(resultSet.getBoolean("HIDDEN"));
        builder.active(true);
        return builder.build();
    }
}
