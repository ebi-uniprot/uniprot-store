package org.uniprot.store.spark.indexer.proteome.converter;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeType;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

import static org.uniprot.store.spark.indexer.common.util.RowUtils.*;

/**
 * Converts XML {@link Row} instances to {@link ProteomeEntry} instances.
 *
 * @author sahmad
 * @created 21/08/2020
 */
public class DatasetProteomeEntryConverter implements Function<Row, ProteomeEntry>, Serializable {

    private static final long serialVersionUID = -6073762696467389831L;

    @Override
    public ProteomeEntry call(Row row) throws Exception {
        ProteomeEntryBuilder builder = new ProteomeEntryBuilder();
        builder.proteomeId(row.getString(row.fieldIndex("upid")));
        // put upid and taxonomy from xml
        Taxonomy taxonomy =
                new TaxonomyBuilder().taxonId(row.getInt(row.fieldIndex("taxonomy"))).build();
        builder.taxonomy(taxonomy);
        // add if is a reference proteome
        ProteomeType type = ProteomeType.NORMAL;
        if(hasFieldName("isReferenceProteome", row)) {
            boolean isReference = row.getBoolean(row.fieldIndex("isReferenceProteome"));
            if(isReference) {
                type = ProteomeType.REFERENCE;
            }
        }
        builder.proteomeType(type);
        return builder.build();
    }
}
