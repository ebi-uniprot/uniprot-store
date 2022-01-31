package org.uniprot.store.spark.indexer.chebi.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class ChebiPH7RelatedMapper implements PairFunction<Row, Long, Long> {
    private static final long serialVersionUID = 8402774616770810020L;

    /**
     * @param row loaded from chebi_pH7_3_mapping.tsv
     * @return Tuple2<RelatedPH7Id,ChebiId>
     * @throws Exception
     */
    @Override
    public Tuple2<Long, Long> call(Row row) throws Exception {
        String fromChebiId = row.getString(row.fieldIndex("CHEBI"));
        String toRelatedId = row.getString(row.fieldIndex("CHEBI_PH7_3"));
        return new Tuple2<>(Long.parseLong(toRelatedId), Long.parseLong(fromChebiId));
    }
}
