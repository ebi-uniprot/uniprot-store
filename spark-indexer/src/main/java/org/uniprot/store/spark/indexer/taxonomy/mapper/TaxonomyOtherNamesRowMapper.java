package org.uniprot.store.spark.indexer.taxonomy.mapper;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;

import scala.Tuple2;

public class TaxonomyOtherNamesRowMapper
        implements PairFunction<Row, String, TaxonomyEntry>, Serializable {

    private static final long serialVersionUID = 454638048148267400L;

    @Override
    public Tuple2<String, TaxonomyEntry> call(Row row) throws Exception {
        BigDecimal taxId = row.getDecimal(row.fieldIndex("TAX_ID"));
        BigDecimal priority = row.getDecimal(row.fieldIndex("PRIORITY"));

        String otherName = "";
        if (priority.longValue() > 0L) {
            otherName = row.getString(row.fieldIndex("NAME"));
        }
        TaxonomyEntry entry =
                new TaxonomyEntryBuilder()
                        .taxonId(taxId.longValue())
                        .otherNamesAdd(otherName)
                        .build();
        return new Tuple2<>(String.valueOf(taxId.longValue()), entry);
    }
}
