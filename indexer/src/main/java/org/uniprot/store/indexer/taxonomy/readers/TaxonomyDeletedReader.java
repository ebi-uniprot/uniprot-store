package org.uniprot.store.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyInactiveReason;
import org.uniprot.core.taxonomy.TaxonomyInactiveReasonType;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.builder.TaxonomyInactiveReasonBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class is mapping {@link ResultSet} returned by SQL executed in
 * {@link org.uniprot.store.indexer.taxonomy.steps.TaxonomyDeletedStep} to {@link TaxonomyEntry} object that
 * will be used to save Deleted Taxonomy Nodes
 *
 * @author lgonzales
 */
public class TaxonomyDeletedReader implements RowMapper<TaxonomyEntry> {

    @Override
    public TaxonomyEntry mapRow(ResultSet resultSet, int i) throws SQLException {
        TaxonomyInactiveReason inactiveReason = new TaxonomyInactiveReasonBuilder()
                .inactiveReasonType(TaxonomyInactiveReasonType.DELETED).build();

        TaxonomyEntry deletedEntry = new TaxonomyEntryBuilder()
                .taxonId(resultSet.getLong("TAX_ID"))
                .inactiveReason(inactiveReason)
                .build();
        return deletedEntry;
    }
}

