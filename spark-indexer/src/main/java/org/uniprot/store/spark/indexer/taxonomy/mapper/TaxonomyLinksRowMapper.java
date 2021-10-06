package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;

public class TaxonomyLinksRowMapper implements PairFunction<Row, String, TaxonomyEntry>, Serializable {

    private static final long serialVersionUID = 7247854332996168658L;

    @Override
    public Tuple2<String, TaxonomyEntry> call(Row row) throws Exception {
        BigDecimal taxId = row.getDecimal(row.fieldIndex("TAX_ID"));
        String link = row.getString(row.fieldIndex("URI"));
        TaxonomyEntry entry = new TaxonomyEntryBuilder()
                .taxonId(taxId.longValue())
                .linksAdd(link)
                .build();
        return new Tuple2<>(String.valueOf(taxId.longValue()), entry);
    }

}
