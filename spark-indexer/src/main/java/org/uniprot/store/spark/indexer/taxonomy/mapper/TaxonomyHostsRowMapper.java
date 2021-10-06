package org.uniprot.store.spark.indexer.taxonomy.mapper;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class TaxonomyHostsRowMapper implements PairFunction<Row, String, String>, Serializable {

    private static final long serialVersionUID = -8988045724090738774L;

    @Override
    public Tuple2<String, String> call(Row row) throws Exception {
        BigDecimal taxId = row.getDecimal(row.fieldIndex("TAX_ID"));
        BigDecimal hostId = row.getDecimal(row.fieldIndex("HOST_ID"));
        return new Tuple2<>(String.valueOf(hostId), String.valueOf(taxId));
    }
}
