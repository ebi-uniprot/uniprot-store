package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;

public class ChebiRelatedIdsJoinMapper
        implements Function<
                Tuple2<ChebiEntry, Optional<Iterable<ChebiEntry>>>, Tuple2<Object, ChebiEntry>> {

    private static final long serialVersionUID = 4353639749078373384L;

    /**
     * This mapper join all related ids into ChebiEntry.
     *
     * @param tuple Tuple2<ChebiEntry, Optional<Iterable<ChebiRelatedEntry>>>
     * @return Tuple2<chebiId, ChebiEntry>
     * @throws Exception
     */
    @Override
    public Tuple2<Object, ChebiEntry> call(Tuple2<ChebiEntry, Optional<Iterable<ChebiEntry>>> tuple)
            throws Exception {
        ChebiEntryBuilder builder = ChebiEntryBuilder.from(tuple._1);
        if (tuple._2.isPresent()) {
            List<ChebiEntry> relatedEntries = new ArrayList<>();
            tuple._2.get().forEach(relatedEntries::add);
            builder.relatedIdsSet(relatedEntries);
        }
        ChebiEntry result = builder.build();
        return new Tuple2<>(Long.parseLong(result.getId()), result);
    }
}
