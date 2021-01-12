package org.uniprot.store.spark.indexer.proteome.converter;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

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
        // put upid and taxonomy from xml just to set it in SuggestDocument
        Taxonomy taxonomy =
                new TaxonomyBuilder().taxonId(row.getInt(row.fieldIndex("taxonomy"))).build();
        builder.taxonomy(taxonomy);
        return builder.build();
    }
}
