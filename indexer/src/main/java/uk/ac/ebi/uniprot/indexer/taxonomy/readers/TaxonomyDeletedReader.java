package uk.ac.ebi.uniprot.indexer.taxonomy.readers;

import org.springframework.jdbc.core.RowMapper;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyInactiveReason;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyInactiveReasonType;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyEntryBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyInactiveReasonBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class is mapping {@link ResultSet} returned by SQL executed in
 * {@link uk.ac.ebi.uniprot.indexer.taxonomy.steps.TaxonomyDeletedStep} to {@link TaxonomyEntry} object that
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

