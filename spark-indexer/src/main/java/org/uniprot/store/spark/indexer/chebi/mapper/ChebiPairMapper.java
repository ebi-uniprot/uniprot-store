package org.uniprot.store.spark.indexer.chebi.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.chebi.ChebiEntry;

import scala.Tuple2;

public class ChebiPairMapper
        implements PairFunction<Tuple2<Object, ChebiEntry>, String, ChebiEntry> {
    private static final long serialVersionUID = -3658534423066775139L;

    @Override
    public Tuple2<String, ChebiEntry> call(Tuple2<Object, ChebiEntry> tuple2) throws Exception {
        if (tuple2._2 != null) {
            return new Tuple2<>(tuple2._2.getId(), tuple2._2);
        } else {
            return new Tuple2<>("", tuple2._2);
        }
    }
}
