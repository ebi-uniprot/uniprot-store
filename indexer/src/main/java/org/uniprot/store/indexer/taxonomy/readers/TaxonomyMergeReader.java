package org.uniprot.store.indexer.taxonomy.readers;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyInactiveReason;
import org.uniprot.core.taxonomy.TaxonomyInactiveReasonType;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyInactiveReasonBuilder;

/**
 * This class is mapping {@link ResultSet} returned by SQL executed in {@link
 * org.uniprot.store.indexer.taxonomy.steps.TaxonomyMergedStep} to {@link TaxonomyEntry} object that
 * will be used to save Merged Taxonomy Nodes
 *
 * @author lgonzales
 */
public class TaxonomyMergeReader implements RowMapper<TaxonomyEntry> {

    @Override
    public TaxonomyEntry mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyInactiveReason inactiveReason =
                new TaxonomyInactiveReasonBuilder()
                        .inactiveReasonType(TaxonomyInactiveReasonType.MERGED)
                        .mergedTo(resultSet.getLong("NEW_TAX_ID"))
                        .build();

        TaxonomyEntry mergedEntry =
                new TaxonomyEntryBuilder()
                        .taxonId(resultSet.getLong("OLD_TAX_ID"))
                        .inactiveReason(inactiveReason)
                        .build();
        return mergedEntry;
    }
}
