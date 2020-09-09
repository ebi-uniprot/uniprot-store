package org.uniprot.store.spark.indexer.proteome.converter;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;

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
        builder.proteomeId(row.getString(row.fieldIndex("_upid")));
        // put name from xml in description field just for setting it in SuggestDocument
        builder.description(row.getString(row.fieldIndex("name")));
        return builder.build();
    }
}
