package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;

public class ChebiRelatedChebiMapper
        implements PairFunction<Tuple2<Long, Tuple2<ChebiEntry, Long>>, Long, ChebiEntry> {

    private static final long serialVersionUID = 3693295363103106430L;

    /**
     * @param tuple Tuple2<RelatedId, Tuple2<RelatedChebiEntry, ChebiId>>> tuple
     * @return Tuple2<ChebiId, RelatedChebiEntry>
     * @throws Exception
     */
    @Override
    public Tuple2<Long, ChebiEntry> call(Tuple2<Long, Tuple2<ChebiEntry, Long>> tuple)
            throws Exception {
        Long chebiId = tuple._2._2;
        ChebiEntry relatedEntry =
                ChebiEntryBuilder.from(tuple._2._1).relatedIdsSet(new ArrayList<>()).build();
        return new Tuple2<>(chebiId, relatedEntry);
    }
}
